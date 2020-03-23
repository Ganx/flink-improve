package org.apache.flink.streaming.connectors.hbase;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@link HBaseMapper} of tuple type implements.
 */
public class HBaseTupleMapper<IN extends Tuple> extends HBaseMapper<IN> {
	private boolean checkTupleFields = true;
	/**
	 * Converts tuple field to  HBase row key bytes. Row key cannot be null.
	 *
	 * @param value
	 * @return row key
	 */
	@Override
	public byte[] getRowKey(IN value) {
		if (checkTupleFields) {
			verifyMapRelation(value);
			checkTupleFields = false;
		}
		byte[] rks = null;
		int i = 0;
		for (Map.Entry<Object, TypeInformation<?>> re : rkMapper.entrySet()) {
			rks = ArrayUtils.addAll(rks, serialize(re.getValue(), value.getField((Integer) re.getKey())));
			if (i < (rkMapper.size() - 1) && !"".equals(getKeySeparator())) {
				rks = ArrayUtils.addAll(rks, serialize(BasicTypeInfo.STRING_TYPE_INFO, getKeySeparator()));
			}
			i ++;
		}
		return rks;
	}
	/**
	 * Given an input value and position return specific column information.
	 *
	 * @param value Input value
	 * @return Specific column information
	 */
	@Override
	List<Tuple3<byte[], byte[], byte[]>> getColumnInfo(IN value) {
		List<Tuple3<byte[], byte[], byte[]>> cl = new ArrayList<>();
		for (Map.Entry<Object, Tuple3<byte[], byte[], TypeInformation<?>>> ce : cfMapper.entrySet()) {
			if (value.getField((Integer) ce.getKey()) != null) {
				cl.add(new Tuple3<byte[], byte[], byte[]>(ce.getValue().f0, ce.getValue().f1, serialize(ce.getValue().f2, value.getField((Integer) ce.getKey()))));
			}
		}
		return cl;
	}
	/**
	 *
	 * Verify tuple fields map to HBase table fields relation.
	 *
	 */
	@Override
	public void verifyMapRelation (IN value) {
		Preconditions.checkArgument(rkMapper != null && rkMapper.size() > 0 && cfMapper != null && cfMapper.size() > 0, "HBaseMapper rkMapper or cfMapper can not be empty.");
		for (Object o : rkMapper.keySet()) {
			Preconditions.checkArgument(o instanceof  Integer, "HBaseTupleMapper inputPosition's type must be Integer.Please check it.");
			int i = (Integer)o;
			Preconditions.checkArgument(i >= 0 && i < value.getArity(), "HBaseTupleMapper rowKey's inputPosition can not be negative or out of range input arity.");
		}
		for (Object o : cfMapper.keySet()) {
			Preconditions.checkArgument(o instanceof  Integer, "HBaseTupleMapper inputPosition's type must be Integer.Please check it.");
			int i = (Integer)o;
			Preconditions.checkArgument(i >= 0 && i < value.getArity(), "HBaseTupleMapper column's inputPosition can not be negative or out of range input arity.");
		}
	}
	@Override
	public void addRowKey(Object[] inputPositions, @Nonnull TypeInformation<?>[] typeInfos) {
		if (inputPositions != null && inputPositions.length > 0) {
			for (Object o : inputPositions) {
				Preconditions.checkArgument(o instanceof  Integer, "HBaseTupleMapper inputPosition's type must be Integer.Please check it.");
			}
		}
		super.addRowKey(inputPositions, typeInfos);
	}
	@Override
	public void addColumn(Object inputPosition, @Nonnull String columnFamily, @Nonnull String columnQualifier, @Nonnull TypeInformation<?> typeInfo){
		if (inputPosition != null) {
			Preconditions.checkArgument(inputPosition instanceof  Integer, "HBaseTupleMapper inputPosition's type must be Integer.Please check it.");
		}
		super.addColumn(inputPosition, columnFamily, columnQualifier, typeInfo);
	}
}
