package org.apache.flink.streaming.connectors.hbase;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link HBaseMapper} of table sink implements.
 */
public class HBaseTableSinkMapper extends HBaseMapper<Row> {
	private boolean checkTupleFields = true;
	@Override
	byte[] getRowKey(Row value) {
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
	@Override
	List<Tuple3<byte[], byte[], byte[]>> getColumnInfo(Row value) {
		List<Tuple3<byte[], byte[], byte[]>> cl = new ArrayList<>();
		for (Map.Entry<Object, Tuple3<byte[], byte[], TypeInformation<?>>> ce : cfMapper.entrySet()) {
			if (value.getField((Integer) ce.getKey()) != null) {
				cl.add(new Tuple3<byte[], byte[], byte[]>(ce.getValue().f0, ce.getValue().f1, serialize(ce.getValue().f2, value.getField((Integer) ce.getKey()))));
			}
		}
		return cl;
	}
	@Override
	void verifyMapRelation(Row value) {
		Preconditions.checkArgument(rkMapper != null && rkMapper.size() > 0 && cfMapper != null && cfMapper.size() > 0, "HBaseMapper rkMapper or cfMapper can not be empty.");
		for (Object o : rkMapper.keySet()) {
			Preconditions.checkArgument(o instanceof Integer || o instanceof String, "HBaseTableSinkMapper inputPosition's type must be Integer or String.Please check it.");
			if (o instanceof Integer) {
				int i = (Integer) o;
				Preconditions.checkArgument(i >= 0 && i < value.getArity(), "HBaseTableSinkMapper rowKey's inputPosition can not be negative or out of range input arity.");
			}
		}
		for (Object o : cfMapper.keySet()) {
			Preconditions.checkArgument(o instanceof Integer || o instanceof String, "HBaseTableSinkMapper inputPosition's type must be Integer or String.Please check it.");
			if (o instanceof Integer) {
				int i = (Integer) o;
				Preconditions.checkArgument(i >= 0 && i < value.getArity(), "HBaseTableSinkMapper column's inputPosition can not be negative or out of range input arity.");
			}
		}
	}
	/**
	 * Mapper dynamic table fields to hbase table fields.
	 *
	 * @param fieldNames dynamic table fields
	 */
	public void prepareFields (String[] fieldNames) {
		Map<Object, TypeInformation<?>> rkm = new HashMap<>();
		int i = -1;
		for (Object o : this.rkMapper.keySet()) {
			if (o instanceof  String) {
				for (int j = 0 ; j < fieldNames.length ; j++) {
					if (o.equals(fieldNames[j])) {
						i = j;
						break;
					}
				}
				if (i != -1) {
					rkm.put(i, this.rkMapper.get(o));
					i = -1;
				}else {
					throw new IllegalArgumentException("HBaseAppendTableSink not match field [" + o + "] please check it.");
				}
			}else {
				rkm.put(o, this.rkMapper.get(o));
			}
		}
		Map<Object, Tuple3<byte[], byte[], TypeInformation<?>>> cfm = new HashMap<>();
		for (Object o : this.cfMapper.keySet()) {
			if (o instanceof  String) {
				for (int j = 0 ; j < fieldNames.length ; j++) {
					if (o.equals(fieldNames[j])) {
						i = j;
						break;
					}
				}
				if (i != -1) {
					cfm.put(i, this.cfMapper.get(o));
					i = -1;
				}else {
					throw new IllegalArgumentException("HBaseAppendTableSink not match field [" + o + "] please check it.");
				}
			}else {
				cfm.put(o, this.cfMapper.get(o));
			}
		}
		this.rkMapper = rkm;
		this.cfMapper = cfm;
	}
	@Override
	public void addRowKey(Object[] inputPositions, @Nonnull TypeInformation<?>[] typeInfos) {
		if (inputPositions != null && inputPositions.length > 0) {
			for (Object o : inputPositions) {
				Preconditions.checkArgument(o instanceof Integer || o instanceof String, "HBaseTableSinkMapper inputPosition's type must be Integer or String.Please check it.");
			}
		}
		super.addRowKey(inputPositions, typeInfos);
	}
	@Override
	public void addColumn(Object inputPosition, @Nonnull String columnFamily, @Nonnull String columnQualifier, @Nonnull TypeInformation<?> typeInfo){
		if (inputPosition != null) {
			Preconditions.checkArgument(inputPosition instanceof Integer || inputPosition instanceof String, "HBaseTableSinkMapper inputPosition's type must be Integer or String.Please check it.");
		}
		super.addColumn(inputPosition, columnFamily, columnQualifier, typeInfo);
	}
}
