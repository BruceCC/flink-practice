package org.leave.flink.practice.login.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.leave.flink.practice.login.bean.LoginEvent;
import org.leave.flink.practice.login.bean.LoginWarning;
import org.leave.flink.practice.login.function.LoginFailMatchPattern;

import java.time.Duration;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 17:07
 * 连续登录失败检测 CEP实现
 */
@Slf4j
public class LoginFailWithCepTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStream<String> sourceStream = env.readTextFile("data/LoginLog.csv");


        KeyedStream<LoginEvent, Long> loginEventStream = sourceStream.map((MapFunction<String, LoginEvent>) value -> {
            try {
                String[] splits = value.split(",");
                LoginEvent loginEvent = new LoginEvent();
                loginEvent.setUserId(Long.parseLong(splits[0].trim()));
                loginEvent.setIp(splits[1].trim());
                loginEvent.setEventType(splits[2].trim().toLowerCase());
                loginEvent.setEventTime(Long.parseLong(splits[3].trim()));
                return loginEvent;
            } catch (Exception e) {
                log.error("dirty data: " + value);
                return null;
            }
        })
                .filter((FilterFunction<LoginEvent>) value -> null != value)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (element, recordTimestamp) -> element.getEventTime()))
                .keyBy((KeySelector<LoginEvent, Long>) value -> value.getUserId());

        // 定义匹配模式
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("begin").where(new IterativeCondition<LoginEvent>() {

            @Override
            public boolean filter(LoginEvent value, Context ctx) throws Exception {
                return "fail".equalsIgnoreCase(value.getEventType());
            }
        }).<LoginEvent>next("next").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return "fail".equalsIgnoreCase(value.getEventType());
            }
        }).within(Time.seconds(2));

        //应用模式
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream, loginFailPattern);

        // select出匹配到的事件
        SingleOutputStreamOperator<LoginWarning> loginFailDataStream = patternStream.select(new LoginFailMatchPattern());

        loginFailDataStream.print("warning");

        env.execute("login fail with cep job");
    }

}
