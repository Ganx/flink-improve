package org;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.UUID;

public class SourceTest1 extends RichSourceFunction<Tuple5<Long,String,Long,String,String>> {

//    private ListState<String> listState;

    public void run(SourceContext sourceContext) throws Exception {
        while (true) {
            sourceContext.collect(Tuple5.of(new Random().nextLong(), UUID.randomUUID().toString(), new Random().nextLong(), UUID.randomUUID().toString(),UUID.randomUUID().toString()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    public void cancel() {

    }


}
