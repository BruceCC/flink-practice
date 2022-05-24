package org.leave.flink.practice.goods.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.leave.flink.practice.goods.bean.ItemViewCount;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


/**
 * @author BruceCC Zhong
 * @date 2022/5/14 19:34
 */
public class TopNHotItemsProcess extends KeyedProcessFunction<Long, ItemViewCount, String> {
    private int topSize;

    public TopNHotItemsProcess(int topSize) {
        this.topSize = topSize;
    }

    private ListState<ItemViewCount> itemState;

    /**
     * 定时器触发时，对所有数据进行排序并输出结果
     */
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        // 将所有state中的数据取出，放到一个List中
        List<ItemViewCount> allItems = new ArrayList<>();
        for (ItemViewCount viewCount : itemState.get()) {
            allItems.add(viewCount);
        }

        // 按照count大小降序排列并取前N个
        allItems.sort((o1, o2) -> o1.getCount().compareTo(o2.getCount()));

        // 清空状态
        itemState.clear();
        // topN 输出
        StringBuilder result = new StringBuilder();
        result.append("current time: ").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < topSize; i++) {
            if (i >= allItems.size()) {
                continue;
            }
            ItemViewCount itemViewCount = allItems.get(i);
            result.append("No").append(i + 1).append(":")
                    .append(" itemId = ").append(itemViewCount.getItemId())
                    .append(" pv = ").append(itemViewCount.getCount()).append("\n");
        }
        System.out.println(result.toString());
        // 控制输出频率
        Thread.sleep(1000);
        out.collect(result.toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        itemState = getRuntimeContext().getListState(new ListStateDescriptor<>("itemState", ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
        // 把每条数据存入状态列表
        itemState.add(value);
        // 注册一个定时器
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }
}
