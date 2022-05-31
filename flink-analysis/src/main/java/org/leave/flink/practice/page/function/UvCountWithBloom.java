package org.leave.flink.practice.page.function;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.page.bean.UvCount;
import org.leave.flink.practice.utils.jedis.RedisUtil;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/25
 */
public class UvCountWithBloom extends ProcessWindowFunction<Tuple2<String, Long>, UvCount, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<UvCount> out) throws Exception {
        RedisUtil redisUtil = RedisUtil.getInstance();
        String storeKey = new Timestamp(context.window().getEnd()).toString();
        long count = Integer.parseInt(redisUtil.hget("uv_count", storeKey));
        Iterator<Tuple2<String, Long>> itor = elements.iterator();
        BloomFilter<Long> bloomFilter = BloomFilter.create(Funnels.longFunnel(), 10000000, 0.0001);
        while (itor.hasNext()) {
            Tuple2<String, Long> element = itor.next();
            if (!bloomFilter.mightContain(element.f1)) {
                bloomFilter.put(element.f1);
                count++;
                redisUtil.hset("uv_count", storeKey, String.valueOf(count));
                out.collect(new UvCount(Long.parseLong(storeKey), count));
            } else {
                out.collect(new UvCount(Long.parseLong(storeKey), count));
            }

        }
    }
}
