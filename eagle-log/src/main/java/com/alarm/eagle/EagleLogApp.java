package com.alarm.eagle;

import com.alarm.eagle.config.ConfigConstant;
import com.alarm.eagle.config.EagleProperties;
import com.alarm.eagle.rule.RuleUtil;
import com.alarm.eagle.sink.es.ElasticsearchUtil;
import com.alarm.eagle.log.*;
import com.alarm.eagle.rule.RuleBase;
import com.alarm.eagle.sink.redis.LogStatAggregateFunction;
import com.alarm.eagle.sink.redis.LogStatWindowFunction;
import com.alarm.eagle.sink.redis.LogStatWindowResult;
import com.alarm.eagle.sink.redis.RedisAggSinkFunction;
import com.alarm.eagle.source.RuleSourceFunction;
import com.alarm.eagle.util.StringUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.CollectionUtil;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 规则的加载与广播：getRuleDataSource 方法负责从外部规则服务动态加载规则，并广播到所有的处理任务中。
 * 日志与规则的连接：BroadcastConnectedStream 将日志流与规则流连接，结合日志数据与动态规则，实现基于规则的处理。
 * 规则驱动的日志处理逻辑：LogProcessFunctionByDrools 应用了规则引擎逻辑，根据规则动态调整日志的处理方式。
 * 动态规则更新：Flink 的广播状态支持在任务运行时更新规则，允许规则实时生效，无需停止任务。
 * <p>
 * Created by luxiaoxun on 2020/01/27.
 */
public class EagleLogApp {
    private static final Logger logger = LoggerFactory.getLogger(EagleLogApp.class);

    public static void main(String[] args) {
        try {
            ParameterTool params = ParameterTool.fromArgs(args);
            ParameterTool parameter = EagleProperties.getInstance(params).getParameter();
            showConf(parameter);

            // Build stream DAG
            StreamExecutionEnvironment env = getStreamExecutionEnvironment(parameter);
            DataStream<LogEvent> dataSource = getKafkaDataSource(parameter, env);
            BroadcastStream<RuleBase> ruleSource = getRuleDataSource(parameter, env);
            BroadcastConnectedStream<LogEvent, RuleBase> connectedStreams = dataSource.connect(ruleSource);
            SingleOutputStreamOperator<LogEvent> processedStream = processLogStream(parameter, connectedStreams);
            sinkToRedis(parameter, processedStream);
            sinkToElasticsearch(parameter, processedStream);

            DataStream<LogEvent> kafkaOutputStream = processedStream.getSideOutput(Descriptors.kafkaOutputTag);
            sinkLogToKafka(parameter, kafkaOutputStream);

            env.getConfig().setGlobalJobParameters(parameter);
            env.execute("eagle-log");
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    private static BroadcastStream<RuleBase> getRuleDataSource(ParameterTool parameter, StreamExecutionEnvironment env) {
        String ruleUrl = parameter.get(ConfigConstant.STREAM_RULE_URL);
        String ruleName = "rules-source";
        return env.addSource(new RuleSourceFunction(ruleUrl)).name(ruleName).uid(ruleName).setParallelism(1).broadcast(Descriptors.ruleStateDescriptor);
    }

    private static DataStream<LogEvent> getKafkaDataSource(ParameterTool parameter, StreamExecutionEnvironment env) {
        String kafkaBootstrapServers = parameter.get(ConfigConstant.KAFKA_BOOTSTRAP_SERVERS);
        String kafkaGroupId = parameter.get(ConfigConstant.KAFKA_GROUP_ID);
        String kafkaTopic = parameter.get(ConfigConstant.KAFKA_TOPIC);
        int kafkaParallelism = parameter.getInt(ConfigConstant.KAFKA_TOPIC_PARALLELISM);
        String kafkaUsername = parameter.get(ConfigConstant.KAFKA_SASL_USERNAME);
        String kafkaPassword = parameter.get(ConfigConstant.KAFKA_SASL_PASSWORD);
        Properties properties = getProperties(kafkaUsername, kafkaPassword);
        KafkaSource<LogEvent> source = KafkaSource.<LogEvent>builder().setBootstrapServers(kafkaBootstrapServers).setTopics(kafkaTopic).setGroupId(kafkaGroupId).setProperties(properties)
                // Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)).setDeserializer(new LogSchema()).build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), kafkaTopic).name(kafkaTopic).uid(kafkaTopic).setParallelism(kafkaParallelism);
    }

    private static SingleOutputStreamOperator<LogEvent> processLogStream(ParameterTool parameter, BroadcastConnectedStream<LogEvent, RuleBase> connectedStreams) throws Exception {
        int processParallelism = parameter.getInt(ConfigConstant.STREAM_PROCESS_PARALLELISM);
        String kafkaIndex = parameter.get(ConfigConstant.KAFKA_SINK_INDEX);
        String ruleUrl = parameter.get(ConfigConstant.STREAM_RULE_URL);
        RuleBase ruleBase = RuleUtil.getMockRules(ruleUrl);
        if (CollectionUtil.isNullOrEmpty(ruleBase.getRules())) {
            throw new Exception("Can not get initial rules");
        } else {
            String name = "process-log";
            logger.debug("Initial rules: " + ruleBase);
            // TODO 用 Flink CEP 替换
            return connectedStreams.process(new LogProcessFunctionByDrools(ruleBase, kafkaIndex)).setParallelism(processParallelism).name(name).uid(name);
        }
    }

    private static SingleOutputStreamOperator<LogEvent> processLogStream(ParameterTool parameter, DataStream<LogEvent> logStream) throws Exception {
        int processParallelism = parameter.getInt(ConfigConstant.STREAM_PROCESS_PARALLELISM);

        // 从 MySQL 中动态加载用户定义的 Flink SQL CEP
        List<String> sqlRules = loadSqlCepRulesFromDatabase();

        // 解析 SQL 规则为 Flink CEP Pattern
        Pattern<LogEvent, ?> pattern = parseSqlToCepPattern(sqlRules);

        // 使用 Flink CEP 创建模式流
        PatternStream<LogEvent> patternStream = CEP.pattern(logStream.keyBy(LogEvent::getId), pattern);

        // 应用模式流的处理逻辑
        String name = "flink-cep-process";
        return patternStream.process(new LogProcessFunctionByFlinkCEP()).setParallelism(processParallelism).name(name).uid(name);
    }

    private static List<String> loadSqlCepRulesFromDatabase() {
        List<String> sqlRules = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(
                "jdbc:mysql://<host>:<port>/<database>",
                "<username>", "<password>")) {
            String query = "SELECT sql_rule FROM cep_rules WHERE status = 'active'";
            try (PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    sqlRules.add(resultSet.getString("sql_rule"));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error loading SQL CEP rules from MySQL", e);
        }
        return sqlRules;
    }

    private static Pattern<LogEvent, ?> parseSqlToCepPattern(List<String> sqlRules) throws Exception {
        if (sqlRules.isEmpty()) {
            throw new Exception("No CEP SQL rules available");
        }

        // 简单示例：将 SQL 翻译为 Pattern
        // 更复杂的实现可以基于 ANTLR 或自定义 SQL 解析器
        Pattern<LogEvent, ?> pattern = Pattern.<LogEvent>begin("start")
                .where(new SimpleCondition<LogEvent>() {
                    @Override
                    public boolean filter(LogEvent logEvent) {
                        // 示例条件：匹配某种规则
                        return logEvent.getType().equals("ERROR");
                    }
                })
                .next("next")
                .where(new SimpleCondition<LogEvent>() {
                    @Override
                    public boolean filter(LogEvent logEvent) {
                        // 示例条件：连续错误日志
                        return logEvent.getType().equals("ERROR") && logEvent.getSeverity() > 5;
                    }
                });

        return pattern;
    }




    private static void sinkLogToKafka(ParameterTool parameter, DataStream<LogEvent> stream) {
        String kafkaBootstrapServers = parameter.get(ConfigConstant.KAFKA_SINK_BOOTSTRAP_SERVERS);
        String kafkaTopic = parameter.get(ConfigConstant.KAFKA_SINK_TOPIC);
        int kafkaParallelism = parameter.getInt(ConfigConstant.KAFKA_SINK_TOPIC_PARALLELISM);
        String kafkaUsername = parameter.get(ConfigConstant.KAFKA_SASL_USERNAME);
        String kafkaPassword = parameter.get(ConfigConstant.KAFKA_SASL_PASSWORD);
        Properties properties = getProperties(kafkaUsername, kafkaPassword);
        String name = "kafka-sink";
        KafkaRecordSerializationSchema<LogEvent> recordSerializationSchema = KafkaRecordSerializationSchema.builder().setTopic(kafkaTopic).setValueSerializationSchema(new LogSchema()).build();
        KafkaSink<LogEvent> kafkaSink = KafkaSink.<LogEvent>builder().setBootstrapServers(kafkaBootstrapServers).setRecordSerializer(recordSerializationSchema).setKafkaProducerConfig(properties).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
        stream.sinkTo(kafkaSink).setParallelism(kafkaParallelism).name(name).uid(name);
    }

    private static void sinkToElasticsearch(ParameterTool parameter, DataStream<LogEvent> dataSource) {
        List<HttpHost> esHttpHosts = ElasticsearchUtil.getEsAddresses(parameter.get(ConfigConstant.ELASTICSEARCH_HOSTS));
        int bulkMaxActions = parameter.getInt(ConfigConstant.ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 5000);
        int bulkMaxSize = parameter.getInt(ConfigConstant.ELASTICSEARCH_BULK_FLUSH_MAX_SIZE_MB, 5);
        int intervalMillis = parameter.getInt(ConfigConstant.ELASTICSEARCH_BULK_FLUSH_INTERVAL_MS, 1000);
        int esSinkParallelism = parameter.getInt(ConfigConstant.ELASTICSEARCH_SINK_PARALLELISM);
        String indexPostfix = parameter.get(ConfigConstant.ELASTICSEARCH_INDEX_POSTFIX, "");

        String name = "ES-sink";
        Sink<LogEvent> esSink = new Elasticsearch7SinkBuilder<LogEvent>().setHosts(esHttpHosts.toArray(new HttpHost[0])).setBulkFlushMaxActions(bulkMaxActions) // Instructs the sink to emit after every element, otherwise they would be buffered
                .setBulkFlushMaxSizeMb(bulkMaxSize).setBulkFlushInterval(intervalMillis).setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 3, 1000).setEmitter((element, context, indexer) -> indexer.add(ElasticsearchUtil.createIndexRequest(element, indexPostfix))).build();
        dataSource.sinkTo(esSink).setParallelism(esSinkParallelism).name(name).uid(name);
    }

    private static void sinkToRedis(ParameterTool parameter, DataStream<LogEvent> dataSource) {
        // save statistic information in redis
        int windowTime = parameter.getInt(ConfigConstant.REDIS_WINDOW_TIME_SECONDS);
        int windowCount = parameter.getInt(ConfigConstant.REDIS_WINDOW_TRIGGER_COUNT);
        int redisSinkParallelism = parameter.getInt(ConfigConstant.REDIS_SINK_PARALLELISM);
        String name = "redis-agg-log";
        DataStream<LogStatWindowResult> keyedStream = dataSource.keyBy((KeySelector<LogEvent, String>) LogEvent::getIndex).window(TumblingProcessingTimeWindows.of(Time.seconds(windowTime))).trigger(new CountTriggerWithTimeout<>(windowCount, TimeCharacteristic.ProcessingTime)).aggregate(new LogStatAggregateFunction(), new LogStatWindowFunction()).setParallelism(redisSinkParallelism).name(name).uid(name);
        String sinkName = "redis-sink";
        keyedStream.addSink(new RedisAggSinkFunction()).setParallelism(redisSinkParallelism).name(sinkName).uid(sinkName);
    }

    private static Properties getProperties(String kafkaUsername, String kafkaPassword) {
        Properties properties = new Properties();
        if (!StringUtil.isEmpty(kafkaUsername) && !StringUtil.isEmpty(kafkaPassword)) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
            String jaasCfg = String.format(jaasTemplate, kafkaUsername, kafkaPassword);
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasCfg);
        }
        return properties;
    }

    private static StreamExecutionEnvironment getStreamExecutionEnvironment(ParameterTool parameter) {
        StreamExecutionEnvironment env = null;
        int globalParallelism = parameter.getInt(ConfigConstant.FLINK_PARALLELISM);
        if (parameter.get(ConfigConstant.FLINK_MODE).equals(ConfigConstant.MODE_DEV)) {
            env = StreamExecutionEnvironment.createLocalEnvironment();
            globalParallelism = 1;
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.setParallelism(globalParallelism);

        //checkpoint
        boolean enableCheckpoint = parameter.getBoolean(ConfigConstant.FLINK_ENABLE_CHECKPOINT, false);
        if (enableCheckpoint) {
            env.enableCheckpointing(60000L);
            CheckpointConfig config = env.getCheckpointConfig();
            config.setMinPauseBetweenCheckpoints(30000L);
            config.setCheckpointTimeout(10000L);
            //RETAIN_ON_CANCELLATION则在job cancel的时候会保留externalized checkpoint state
            config.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        }

        return env;
    }

    private static void showConf(ParameterTool parameter) {
        logger.info("Show " + parameter.getNumberOfParameters() + " config parameters");
        Configuration configuration = parameter.getConfiguration();
        Map<String, String> map = configuration.toMap();
        for (String key : map.keySet()) {
            logger.info(key + ":" + map.get(key));
        }
    }
}
