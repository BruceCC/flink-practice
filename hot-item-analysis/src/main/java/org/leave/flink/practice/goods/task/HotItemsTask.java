package org.leave.flink.practice.goods.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.leave.flink.practice.goods.bean.ItemViewCount;
import org.leave.flink.practice.goods.bean.UserBehavior;
import org.leave.flink.practice.goods.constant.UserBehaviorConstant;
import org.leave.flink.practice.goods.function.ItemViewCountAgg;
import org.leave.flink.practice.goods.function.TopNHotItemsProcess;
import org.leave.flink.practice.goods.function.ItemViewWindowResult;

import java.time.Duration;
import java.util.Properties;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/7
 * <p>
 * 近一个小时内热门商品topN
 */
@Slf4j
public class HotItemsTask {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //定义source数据源
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "hotitems");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("hotitems")
                .setGroupId("hotitems-count")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        //获取数据源
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(1);
        //获取PV数据
        SingleOutputStreamOperator<UserBehavior> dataStream = sourceStream.map((MapFunction<String, UserBehavior>) value -> {
            try {
                String[] splits = value.split(",");
                return new UserBehavior(Long.parseLong(splits[0].trim()), Long.parseLong(splits[1].trim()), Integer.parseInt(splits[2].trim()), splits[3].trim().toLowerCase(), Long.parseLong(splits[4].trim()) * 1000L);
            } catch (Exception e) {
                log.error("Dirty data: " + value);
                return null;
            }
        })
                .filter((FilterFunction<UserBehavior>) value -> null != value)
                .filter((FilterFunction<UserBehavior>) value -> UserBehaviorConstant.PV.equals(value.getBehavior()))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>) (element, recordTimestamp) -> element.getTimestamp()));

        //看热门商品
        SingleOutputStreamOperator<String> processedStream = dataStream.keyBy((KeySelector<UserBehavior, Long>) value -> value.getItemId())
                //.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                //窗口聚合
                //WindowResultAgg输入数据来自CountAgg，在窗口结束的时候先执行Aggregate对象的getResult，然后再执行 自己的apply对象
                .aggregate(new ItemViewCountAgg(), new ItemViewWindowResult())
                //按照窗口分组
                .keyBy((KeySelector<ItemViewCount, Long>) value -> value.getWindowEnd())
                .process(new TopNHotItemsProcess(5));

        // sink
        processedStream.print();

        env.execute("hot items job");

    }
}
