package org.leave.flink.practice.market.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.market.bean.AppMarketViewCount;

import java.sql.Timestamp;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 */
public class AppMarketCountByChannel extends ProcessWindowFunction<Tuple2<Tuple2<String, String>, Long>, AppMarketViewCount, Tuple2<String, String>, TimeWindow> {
    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<Tuple2<Tuple2<String, String>, Long>> elements, Collector<AppMarketViewCount> out) throws Exception {
        long count = 0;
        while (elements.iterator().hasNext()) {
            elements.iterator().next();
            count++;
        }
        out.collect(new AppMarketViewCount(new Timestamp(context.window().getStart()).toString(), new Timestamp(context.window().getEnd()).toString(), key.f0, key.f1, count));
    }
}
