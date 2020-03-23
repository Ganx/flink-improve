package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps a input value to a row in HBase table.
 *
 * @param <IN> input type
 */
public abstract class HBaseMapper<IN> implements Serializable {
	private static final long serialVersionUID = 10010L;
	protected Map<Object, Tuple3<byte[], byte[], TypeInformation<?>>> cfMapper;
	protected Map<Object, TypeInformation<?>> rkMapper;
	private String KeySeparator = "";

	public void setKeySeparator(String keySeparator) {
		KeySeparator = keySeparator;
	}

	public String getKeySeparator() {
		return KeySeparator;
	}

	/**
	 * Mapper input field to rowKey.
	 *
	 * @param typeInfo Input field type
	 *
	 */
	public void addRowKey(@Nonnull TypeInformation<?> typeInfo) {
		TypeInformation[] ts = {typeInfo};
		addRowKey(null, ts);
	}
	/**
	 * Mapper input field to rowKey.
	 *
	 * @param typeInfos Input fields type
	 *
	 */
	public void addRowKey(@Nonnull TypeInformation<?>[] typeInfos) {
		addRowKey(null, typeInfos);
	}
	/**
	 * Mapper input field to rowKey.
	 *
	 * @param inputPosition Mapper to input field position
	 * @param typeInfo Input field type
	 *
	 */
	public void addRowKey(Object inputPosition, @Nonnull TypeInformation<?> typeInfo) {
		TypeInformation[] ts = {typeInfo};
		Object[] is = {inputPosition};
		addRowKey(is, ts);
	}
	/**
	 * Mapper input field to rowKey.
	 *
	 * @param inputPositions Mapper to input fields position
	 * @param typeInfos Input fields type, suppoet union fields mapper to rowKey
	 *
	 */
	public void addRowKey(Object[] inputPositions, @Nonnull TypeInformation<?>[] typeInfos) {
		Preconditions.checkArgument(typeInfos.length > 0, "HBaseMapper add rowKey typeInfos can not be empty.");
		Preconditions.checkArgument(inputPositions == null || inputPositions.length == typeInfos.length, "HBaseMapper add rowKey typeInfos's length must be equal inputPositions's length.");
		if (rkMapper == null) {
			rkMapper = new LinkedHashMap<>();
		}
		if (inputPositions == null) {
			for (int i =0 ; i < typeInfos.length ; i++) {
				rkMapper.put(i, typeInfos[i]);
			}
		}else {
			for (int i =0 ; i < typeInfos.length ; i++) {
				rkMapper.put(inputPositions[i], typeInfos[i]);
			}
		}
	}
	/**
	 * Mapper input fields to columnFamily qualifier in order.
	 *
	 * @param columnFamily Hbase table columnFamily
	 * @param columnQualifier Hbase table columnFamily qualifier
	 * @param typeInfo Input field type
	 *
	 */
	public void addColumn(@Nonnull String columnFamily, @Nonnull String columnQualifier, @Nonnull TypeInformation<?> typeInfo){
		addColumn(null, columnFamily, columnQualifier, typeInfo);
	}
	/**
	 * Mapper input fields to columnFamily qualifier in order.
	 *
	 * @param inputPosition Mapper to input fields position
	 * @param columnFamily Hbase table columnFamily
	 * @param columnQualifier Hbase table columnFamily qualifier
	 * @param typeInfo Input field type
	 *
	 */
	public void addColumn(Object inputPosition, @Nonnull String columnFamily, @Nonnull String columnQualifier, @Nonnull TypeInformation<?> typeInfo){
		Preconditions.checkArgument(columnFamily.length() > 0, "HBaseMapper add column columnFamily can not be empty.");
		Preconditions.checkArgument(columnQualifier.length() > 0, "HBaseMapper add column columnQualifier can not be empty.");
		if (cfMapper == null) {
			cfMapper = new LinkedHashMap<>();
		}
		Tuple3<byte[], byte[], TypeInformation<?>> t = new Tuple3<>();
		t.f0 = Bytes.toBytes(columnFamily);
		t.f1 = Bytes.toBytes(columnQualifier);
		t.f2 = typeInfo;
		if (inputPosition == null) {
			int i = cfMapper.size();
			cfMapper.put(i, t);
		}else {
			cfMapper.put(inputPosition, t);
		}
	}
	/**
	 * Serializes the given value to a bytes
	 *
	 * @param typeInfo Serializes type
	 * @param o Serializes value
	 * @return
	 */
	protected byte[] serialize (TypeInformation<?> typeInfo, Object o) {
		byte[] se = null;
		if (o != null) {
			Class<?> clazz = typeInfo.getTypeClass();
			if (byte[].class.equals(clazz)) {
				se = (byte[]) o;
			} else if (String.class.equals(clazz)) {
				se = Bytes.toBytes((String) o);
			} else if (Byte.class.equals(clazz)) {
				se = Bytes.toBytes((Byte) o);
			} else if (Short.class.equals(clazz)) {
				se = Bytes.toBytes((Short) o);
			} else if (Integer.class.equals(clazz)) {
				se = Bytes.toBytes((Integer) o);
			} else if (Long.class.equals(clazz)) {
				se = Bytes.toBytes((Long) o);
			} else if (Float.class.equals(clazz)) {
				se = Bytes.toBytes((Float) o);
			} else if (Double.class.equals(clazz)) {
				se = Bytes.toBytes((Double) o);
			} else if (Boolean.class.equals(clazz)) {
				se = Bytes.toBytes((Boolean) o);
			} else if (BigDecimal.class.equals(clazz)) {
				se = Bytes.toBytes((BigDecimal) o);
			} else {
				throw new IllegalArgumentException("HBaseMapper not support this " + clazz + " type.");
			}
		}
		return se;
	}
	/**
	 * Given an input value return the HBase row key bytes. Row key cannot be null.
	 *
	 * @param value Get rowKey from value
	 * @return row key
	 */
	abstract byte[] getRowKey(IN value);
	/**
	 * Given an input value and position return specific column information.
	 *
	 * @param value Input value
	 * @return Specific column information
	 */
	abstract List<Tuple3<byte[], byte[], byte[]>> getColumnInfo (IN value);
	/**
	 *
	 * Verify input fields map to HBase table fields relation.
	 *
	 */
	abstract void verifyMapRelation (IN value);
}
