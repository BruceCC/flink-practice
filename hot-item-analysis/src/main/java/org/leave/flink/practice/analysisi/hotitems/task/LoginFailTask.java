package org.leave.flink.practice.analysisi.hotitems.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.leave.flink.practice.analysisi.hotitems.bean.LoginEvent;
import org.leave.flink.practice.analysisi.hotitems.bean.LoginWarning;
import org.leave.flink.practice.analysisi.hotitems.bean.UserBehavior;
import org.leave.flink.practice.analysisi.hotitems.function.LoginWarningProcess;

import java.time.Duration;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:22
 * 连续登录失败事件检测
 */
@Slf4j
public class LoginFailTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStream<String> sourceStream = env.readTextFile("data/LoginLog.csv");

        SingleOutputStreamOperator<LoginEvent> loginEventStream = sourceStream.map((MapFunction<String, LoginEvent>) value -> {
            try {
                String[] splits = value.split(",");
                LoginEvent loginEvent = new LoginEvent();
                loginEvent.setUserId(Long.parseLong(splits[0].trim()));
                loginEvent.setIp(splits[1].trim());
                loginEvent.setEventType(splits[2].trim().toLowerCase());
                loginEvent.setEventTime(Long.parseLong(splits[3].trim()));
                return loginEvent;
            } catch (Exception e) {
                log.error("dirty record, data: " + value);
                return null;
            }
        })
                .filter((FilterFunction<LoginEvent>) value -> null != value)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (element, recordTimestamp) -> element.getEventTime()));


        SingleOutputStreamOperator<LoginWarning> warningStream = loginEventStream.keyBy((KeySelector<LoginEvent, Long>) value -> value.getUserId())
                .process(new LoginWarningProcess(2));

        warningStream.print("warning");

        env.execute("login fail detect job");
    }
}
