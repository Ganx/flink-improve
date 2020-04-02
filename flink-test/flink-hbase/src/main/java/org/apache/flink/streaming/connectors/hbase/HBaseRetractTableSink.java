/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;

/**
 * An {@link RetractStreamTableSink} to write an retract stream Table to a HBase table.
 */
public class HBaseRetractTableSink implements RetractStreamTableSink<Row>,Serializable {
	private static final long serialVersionUID = 818L;
	private final HBaseTableSinkMapper hbMapper;
	private final Map<String, Object> hbConfig;
	private String tableOwner;
	private final String tableName;
	private MutationActions.ProcessMode pMode;
	private Boolean isWriteAheadLogEnabled = false;
	private Boolean logRowkeyNullOnly = false;
	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;
	public HBaseRetractTableSink(String tName,
								 String tOwner,
								 Map<String, Object> hbConfig,
								 HBaseTableSinkMapper hbMapper) {
		this(tName, tOwner, hbConfig, hbMapper, null, false, false);
	}
	public HBaseRetractTableSink(String tName,
								 String tOwner,
								 Map<String, Object> hbConfig,
								 HBaseTableSinkMapper hbMapper,
								 MutationActions.ProcessMode pMode) {
		this(tName, tOwner, hbConfig, hbMapper, pMode, false, false);
	}
	public HBaseRetractTableSink(String tName,
								 String tOwner,
								 Map<String, Object> hbConfig,
								 HBaseTableSinkMapper hbMapper,
								 Boolean logRowkeyNullOnly) {
		this(tName, tOwner, hbConfig, hbMapper, null, logRowkeyNullOnly, false);
	}
	public HBaseRetractTableSink(String tName,
								 String tOwner,
								 Map<String, Object> hbConfig,
								 HBaseTableSinkMapper hbMapper,
								 MutationActions.ProcessMode pMode,
								 Boolean logRowkeyNullOnly,
								 Boolean isWriteAheadLogEnabled) {
		Preconditions.checkArgument(tName != null && !tName.isEmpty(),
			"Table name cannot be null or empty.");
		Preconditions.checkArgument(hbConfig != null,
			"HBase config cannot be null.");
		HBaseSink.checkHBaseMapper(hbMapper);
		this.tableName = tName;
		this.tableOwner = tOwner;
		this.hbConfig = hbConfig;
		this.hbMapper = hbMapper;
		this.pMode = pMode;
		if (logRowkeyNullOnly != null) {
			this.logRowkeyNullOnly = logRowkeyNullOnly;
		}
		if (isWriteAheadLogEnabled != null) {
			this.isWriteAheadLogEnabled = isWriteAheadLogEnabled;
		}
	}
	@Override
	public TypeInformation<Row> getRecordType() {
		return new RowTypeInfo(fieldTypes);
	}
	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		try {
			HBaseSink.HBaseSinkBuilder<Row> hsb = HBaseSink.addSink(RetractStreamFilter.filter(dataStream));
			if (isWriteAheadLogEnabled) {
				hsb.enableWriteAheadLog();
			}
			hsb.setTableName(tableName)
				.setTableOwner(tableOwner)
				.setHBaseMapper(hbMapper)
				.setHBaseConfig(hbConfig)
				.setProcessMode(pMode)
				.setLogRowkeyNullOnly(logRowkeyNullOnly)
				.build();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	@Override
	public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
		return new TupleTypeInfo<>(BasicTypeInfo.BOOLEAN_TYPE_INFO, new RowTypeInfo(fieldTypes));
	}
	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}
	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}
	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		Preconditions.checkArgument(fieldNames != null && fieldNames.length > 0,
			"HBaseRetractTableSink fieldNames can not be empty.");
		Preconditions.checkArgument(fieldTypes != null && fieldTypes.length > 0,
			"HBaseRetractTableSink fieldTypes can not be empty.");
		Preconditions.checkArgument(fieldTypes.length == fieldTypes.length,
			"HBaseRetractTableSink number of provided field names and types does not match..");
		HBaseRetractTableSink hBaseRetractTableSink =
			new HBaseRetractTableSink(tableName, tableOwner, hbConfig,
				hbMapper, pMode, logRowkeyNullOnly, isWriteAheadLogEnabled);
		hBaseRetractTableSink.fieldNames = fieldNames;
		hBaseRetractTableSink.fieldTypes = fieldTypes;
		this.hbMapper.prepareFields(fieldNames);
		return hBaseRetractTableSink;
	}
	/**
	 * The insertion is idempotent for same rowkey of hbase table,so we filter retract value.
	 */
	private static class RetractStreamFilter {
		public static DataStream<Row> filter (DataStream<Tuple2<Boolean, Row>> dataStream) {
			return dataStream.map(new RetractMap()).filter(new RetractFilter());
		}
		private static class RetractMap implements MapFunction<Tuple2<Boolean, Row>, Row> {
			@Override
			public Row map(Tuple2<Boolean, Row> value) throws Exception {
				if (value.f0) {
					return value.f1;
				}else {
					return null;
				}
			}
		}
		private static class RetractFilter implements FilterFunction<Row> {
			@Override
			public boolean filter(Row value) throws Exception {
				if (value == null) {
					return false;
				}else {
					return true;
				}
			}
		}
	}
}
