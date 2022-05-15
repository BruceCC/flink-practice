package org.leave.flink.practice.analysisi.hotitems.bean;

import lombok.Data;
import org.apache.kafka.common.protocol.types.Field;

/**
 * @author BruceCC Zhong
 * @date 2022/5/15 12:07
 * 黑名单报警信息
 */
@Data
public class BlackListWarning {
    private Long userId;
    private Long adId;
    private String msg;
}
