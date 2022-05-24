package org.leave.flink.practice.ad.function;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.ad.bean.AdCountByProvince;

import java.sql.Timestamp;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 * flink WindowFunction未来版本中将废弃，因此此处改用 ProcessWindowFunction
 */
public class AdCountResult extends ProcessWindowFunction<Long, AdCountByProvince, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<Long> elements, Collector<AdCountByProvince> out) throws Exception {
        out.collect(new AdCountByProvince(new Timestamp(context.window().getStart()).toString(), new Timestamp(context.window().getEnd()).toString(), key, elements.iterator().next()));
    }
}
