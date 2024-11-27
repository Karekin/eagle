package com.alarm.eagle.log;

import com.alarm.eagle.drools.LogProcessor;
import com.alarm.eagle.drools.LogProcessorWithRules;
import com.alarm.eagle.rule.RuleBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by luxiaoxun on 2020/01/27.
 */
public class LogProcessFunctionByFlinkCEP extends PatternProcessFunction<LogEvent, LogEvent> {
    @Override
    public void processMatch(
            Map<String, List<LogEvent>> match,
            Context ctx,
            Collector<LogEvent> out
    ) {
        // 根据命名的模式匹配组处理结果
        List<LogEvent> startEvents = match.get("start");
        List<LogEvent> nextEvents = match.get("next");

        for (LogEvent start : startEvents) {
            for (LogEvent next : nextEvents) {
                LogEvent alert = new LogEvent();
                alert.setId(UUID.randomUUID().toString());
                alert.setType("ALERT");
                alert.setMessage("Pattern Matched: " + start.getMessage() + " -> " + next.getMessage());
                out.collect(alert);
            }
        }
    }
}

