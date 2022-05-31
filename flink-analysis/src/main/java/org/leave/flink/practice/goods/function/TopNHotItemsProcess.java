package org.leave.flink.practice.goods.function;

import org.apache.commons.compress.utils.Lists;
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

    private ListState<ItemViewCount> itemViewCountListState;

    /**
     * 定时器触发时，对所有数据进行排序并输出结果
     */
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        // 将所有state中的数据取出，放到一个List中
        List<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
        /*List<ItemViewCount> itemViewCounts = new ArrayList<>();
        for (ItemViewCount viewCount : itemViewCountListState.get()) {
            itemViewCounts .add(viewCount);
        }*/

        // 从多到少(越热门越前面)
        itemViewCounts .sort((o1, o2) -> o1.getCount().compareTo(o2.getCount()));

        // TODO 清空状态
        itemViewCountListState.clear();
        // topN 输出
        StringBuilder result = new StringBuilder();
        result.append("============================").append(System.lineSeparator());
        result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append(System.lineSeparator());
        for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
            ItemViewCount itemViewCount = itemViewCounts .get(i);
            result.append("No").append(i + 1).append(":")
                    .append(" 商品ID = ").append(itemViewCount.getItemId())
                    .append(" 热门度 = ").append(itemViewCount.getCount()).append(System.lineSeparator());
        }
        result.append("============================").append(System.lineSeparator());
        System.out.println(result.toString());
        // 控制输出频率
        Thread.sleep(1000L);
        out.collect(result.toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("item-view-count-list", ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
        // 把每条数据存入状态列表
        itemViewCountListState.add(value);
        // 注册一个定时器模拟等待，所以这里时间设的比较短(1ms)
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }
}
