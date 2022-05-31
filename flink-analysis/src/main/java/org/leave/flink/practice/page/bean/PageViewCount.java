package org.leave.flink.practice.page.bean;

import lombok.Data;

/**
 * @Author BruceCC Zhong
 * @date 2022/5/31
 */
@Data
public class PageViewCount {
    private String url;
    private Long windowEnd;
    private Long count;

    public PageViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }
}
