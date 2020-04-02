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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;

/**
 * An {@link AppendStreamTableSink} to write an append stream Table to a HBase table.
 */
public class HBaseAppendTableSink implements AppendStreamTableSink<Row>,Serializable{
	private static final long serialVersionUID = 518L;
	private HBaseTableSinkMapper hbMapper;
	private final Map<String, Object> hbConfig;
	private String tableOwner;
	private final String tableName;
	private MutationActions.ProcessMode pMode;
	private Boolean isWriteAheadLogEnabled = false;
	private Boolean logRowkeyNullOnly = false;
	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;
	public HBaseAppendTableSink(String tName,
								String tOwner,
								Map<String, Object> hbConfig,
								HBaseTableSinkMapper hbMapper) {
		this(tName, tOwner, hbConfig, hbMapper, null, false, false);
	}
	public HBaseAppendTableSink(String tName,
								String tOwner,
								Map<String, Object> hbConfig,
								HBaseTableSinkMapper hbMapper,
								MutationActions.ProcessMode pMode) {
		this(tName, tOwner, hbConfig, hbMapper, pMode, false, false);
	}
	public HBaseAppendTableSink(String tName,
								String tOwner,
								Map<String, Object> hbConfig,
								HBaseTableSinkMapper hbMapper,
								Boolean logRowkeyNullOnly) {
		this(tName, tOwner, hbConfig, hbMapper, null, logRowkeyNullOnly, false);
	}
	public HBaseAppendTableSink(String tName,
								String tOwner,
								Map<String, Object> hbConfig,
								HBaseTableSinkMapper hbMapper,
								MutationActions.ProcessMode pMode,
								Boolean logRowkeyNullOnly,
								Boolean isWriteAheadLogEnabled) {
		Preconditions.checkArgument(tName != null && !tName.isEmpty(),
			"HBaseAppendTableSink table name cannot be null or empty.");
		Preconditions.checkArgument(hbConfig != null,
			"HBaseAppendTableSink hbase config cannot be null.");
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
	public void emitDataStream(DataStream<Row> dataStream) {
		try {
			HBaseSink.HBaseSinkBuilder<Row> hsb = HBaseSink.addSink(dataStream);
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
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(fieldTypes);
	}
	@Override
	public String[] getFieldNames() {
		return this.fieldNames;
	}
	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return this.fieldTypes;
	}
	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		Preconditions.checkArgument(fieldNames != null && fieldNames.length > 0,
			"HBaseAppendTableSink fieldNames can not be empty.");
		Preconditions.checkArgument(fieldTypes != null && fieldTypes.length > 0,
			"HBaseAppendTableSink fieldTypes can not be empty.");
		Preconditions.checkArgument(fieldTypes.length == fieldTypes.length,
			"HBaseAppendTableSink number of provided field names and types does not match..");
		HBaseAppendTableSink hbaseAppendTableSink =
			new HBaseAppendTableSink(tableName, tableOwner, hbConfig,
				hbMapper, pMode, logRowkeyNullOnly, isWriteAheadLogEnabled);
		hbaseAppendTableSink.fieldNames = fieldNames;
		hbaseAppendTableSink.fieldTypes = fieldTypes;
		this.hbMapper.prepareFields(fieldNames);
		return hbaseAppendTableSink;
	}
}
