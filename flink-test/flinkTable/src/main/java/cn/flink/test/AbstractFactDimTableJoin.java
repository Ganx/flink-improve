package cn.flink.test;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 应用模块名称.
 * <p>
 * 代码描述
 * </P>
 *
 * @author xx
 * @since 2020/3/17 16:08
 */
public abstract class AbstractFactDimTableJoin<IN1, IN2, OUT> extends CoProcessFunction<IN1, Dimension, OUT> {
    protected transient ValueState<Dimension> dimState;

    @Override
    public void processElement1(IN1 in1, Context context, Collector<OUT> collector) throws Exception {
        Dimension dim = dimState.value();
       if (dim == null){
           return;
       }
       collector.collect(join(in1,dim));
    }

    abstract OUT join(IN1 value, Dimension dimension);

    @Override
    public void processElement2(Dimension in2, Context context, Collector<OUT> collector) throws Exception {
        dimState.update(in2);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor dimStateDesc =
                new ValueStateDescriptor<>("dimstate", Dimension.class);
        this.dimState = getRuntimeContext().getState(dimStateDesc);
    }
}
