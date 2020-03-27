package com.flink.dim;

import lombok.Data;
import org.apache.flink.table.sources.StreamTableSource;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 应用模块名称.
 * <p>
 * 代码描述
 * </P>
 *
 * @author xx
 * @since 2020/3/24 12:37
 */
@Data
public abstract class DimensionTableSource implements StreamTableSource, Serializable {
    private ArrayList<String> primaryKey = new ArrayList<String>();
    private CacheType cacheType = CacheType.LRU;
    private Integer cacheTTLMinutes;
    private Integer cacheMaximumSize;
    private Boolean enableRemoveLog = false;
    private ArrayList<String> timers = new ArrayList<String>();
    private Boolean enableCleanup = false;
    protected DBConnection dbConnection;
}
