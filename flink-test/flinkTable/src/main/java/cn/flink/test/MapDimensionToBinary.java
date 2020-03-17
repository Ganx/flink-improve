package cn.flink.test;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;

/**
 * 应用模块名称.
 * <p>
 * 代码描述
 * </P>
 *
 * @author xx
 * @since 2020/3/17 16:29
 */
public class MapDimensionToBinary extends RichMapFunction<Dimension,Entity> {

    private transient TypeSerializer dimSerializer;
    private transient DataOutputSerializer serializationBuffer;

    @Override
    public Entity map(Dimension in) throws Exception {
        return null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        serializationBuffer = new DataOutputSerializer(100);
        dimSerializer = TypeInformation.of(Dimension.class).createSerializer(getRuntimeContext().getExecutionConfig());
    }
}
