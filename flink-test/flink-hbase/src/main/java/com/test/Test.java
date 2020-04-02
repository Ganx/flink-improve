package com.test;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.hbase.HBaseMapper;
import org.apache.flink.streaming.connectors.hbase.HBaseSink;
import org.apache.flink.streaming.connectors.hbase.HBaseTupleMapper;
import org.apache.flink.streaming.connectors.hbase.MutationActions;
import org.apache.hadoop.hbase.HConstants;

import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple5<Long, String, Long, String, String>> tuple4DataStreamSource = env.addSource(new SourceTest1());

        MutationActions.ProcessMode pMode = new MutationActions.ProcessMode(true, 2097152, 1000, 5 * 1000);//构建幂等HBaseSink的处理模式
        HBaseMapper<Tuple5<Long, String, Long, String, String>> hMapper = new HBaseTupleMapper<>();//构建 HBaseMapper // 不指定position则默认按tuple或row的顺序依次取值，因此不加position的时候要注意type的顺序
        hMapper.addRowKey(0, BasicTypeInfo.LONG_TYPE_INFO);
        hMapper.addRowKey(3, BasicTypeInfo.STRING_TYPE_INFO);
        hMapper.setKeySeparator("‐");//指定多字段联合rowkey的分隔符，默认没有分隔符

        hMapper.addColumn(0, "f", "id", BasicTypeInfo.LONG_TYPE_INFO);
        hMapper.addColumn(1, "f", "name", BasicTypeInfo.STRING_TYPE_INFO);
        hMapper.addColumn(2, "f", "age", BasicTypeInfo.LONG_TYPE_INFO);
        hMapper.addColumn(3, "f", "sex", BasicTypeInfo.STRING_TYPE_INFO);
        hMapper.addColumn(4, "f", "address", BasicTypeInfo.STRING_TYPE_INFO);

        Map<String, Object> hbConfig = new HashMap<>();//HBase相关的配置
        hbConfig.put(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
        hbConfig.put(HConstants.ZOOKEEPER_CLIENT_PORT,2181);
        hbConfig.put(HConstants.ZOOKEEPER_ZNODE_PARENT,"/hbase");

        HBaseSink.addSink(tuple4DataStreamSource).setHBaseConfig(hbConfig)//指定HBase配置项    
                .setHBaseMapper(hMapper)//指定HBaseMapper   
                .setTableName("nametest:test")//指定表名    
                //.setTableOwner("tableOwner")//指定表所属用户，没有可以不指定  
                .setProcessMode(pMode)//指定ProcessMode    
                .setLogRowkeyNullOnly(true)//true表示rowKey为null时只打印日志不会抛异常，默认为false ；对于列字段为null时不会被插入到hbase系统    
                .build();//构建HBaseSink

        env.execute("");
    }
}
