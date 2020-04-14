package org;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connectors.kudu.connector.KuduColumnInfo;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduInputSplit;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderIterator;
import org.apache.flink.connectors.kudu.connector.serde.DefaultSerDe;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kudu.Type;
import org.junit.Test;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test1 {

    private final static  String TABLENAME = "flink-test-kudu4";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple5<Long, String, Long, String, String>> source = env.addSource(new SourceTest1());
        SingleOutputStreamOperator<KuduRow> map = source.map(new MapFunction<Tuple5<Long, String, Long, String, String>, KuduRow>() {
            @Override
            public KuduRow map(Tuple5<Long, String, Long, String, String> tuple5) throws Exception {
                KuduRow values = new KuduRow(5);
                values.setField(0, "id", tuple5.f0);
                values.setField(1, "title", tuple5.f1);
                values.setField(2, "age", tuple5.f2);
                values.setField(3, "address", tuple5.f3);
                values.setField(4, "sex", tuple5.f4);
                return values;
            }
        });

        KuduTableInfo tableInfo = booksTableInfo(TABLENAME, true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters("incubator-t3-infra02:7051,incubator-t3-infra03:7051")
                .setManualConsistency()
                .build();

        KuduSink<KuduRow> sink = new KuduSink<>(writerConfig, tableInfo, new DefaultSerDe());
        map.addSink(sink);
        env.execute("");
    }

    protected static KuduTableInfo booksTableInfo(String tableName, boolean createIfNotExist) {
        return KuduTableInfo.Builder
                .create(tableName)
                .createIfNotExist(createIfNotExist)
                .replicas(1)
                .addColumn(KuduColumnInfo.Builder.create("id", Type.INT64).key(true).hashKey(true).build())
                .addColumn(KuduColumnInfo.Builder.create("title", Type.STRING).key(true).hashKey(true).build())
                .addColumn(KuduColumnInfo.Builder.create("age", Type.INT64).build())
                .addColumn(KuduColumnInfo.Builder.create("address", Type.STRING).asNullable().build())
                .addColumn(KuduColumnInfo.Builder.create("sex", Type.STRING).asNullable().build())
                .build();
    }


    @Test
    public void read() throws IOException {
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters("incubator-t3-infra02:7051,incubator-t3-infra03:7051").build();

        KuduTableInfo tableInfo = booksTableInfo(TABLENAME, false);

        KuduReader reader = new KuduReader(tableInfo, readerConfig);

        KuduInputSplit[] splits = reader.createInputSplits(1);
        List<KuduRow> rows = new ArrayList<>();
        for (KuduInputSplit split : splits) {
            KuduReaderIterator resultIterator = reader.scanner(split.getScanToken());
            while(resultIterator.hasNext()) {
                KuduRow row = resultIterator.next();
                if(row != null) {
                    rows.add(row);
                }
            }
        }
        reader.close();

        System.out.println(rows);

    }
}
