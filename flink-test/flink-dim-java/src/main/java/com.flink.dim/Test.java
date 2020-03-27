package com.flink.dim;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;

/**
 * 应用模块名称.
 * <p>
 * 代码描述
 * </P>
 *
 * @author xx
 * @since 2020/3/24 13:52
 */
public class Test extends DimensionTableSource{
    public static void main(String[] args) {
       CacheType cacheType = CacheType.ALL;
       Test test = new Test();
        System.out.println(test.getPrimaryKey());
    }

    public DataStream getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        return null;
    }

    public TableSchema getTableSchema() {
        return null;
    }
}
