package org.leave.flink.practice.page.function;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.goods.bean.UserBehavior;
import org.leave.flink.practice.page.bean.PageViewCount;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/24
 */
public class UvCountByWindow implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
        Set<Long> userIdSet = new HashSet<>();
        Iterator<UserBehavior> itor = values.iterator();
        while (itor.hasNext()) {
            UserBehavior userBehavior = itor.next();
            userIdSet.add(userBehavior.getUserId());
        }

        for (UserBehavior userBehavior: values) {
            userIdSet.add(userBehavior.getUserId());
        }

        out.collect(new PageViewCount("uv", window.getEnd(), (long) userIdSet.size()));
    }
}
