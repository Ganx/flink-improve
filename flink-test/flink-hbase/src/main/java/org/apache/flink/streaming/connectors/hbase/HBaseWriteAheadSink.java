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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Sink that emits its input elements into a HBase database. This sink stores incoming records within a
 * {@link org.apache.flink.runtime.state.AbstractStateBackend}, and only commits them to HBase
 * if a checkpoint is completed.
 *
 * @param <IN> Tuple type of the elements emitted by this sink
 */
public class HBaseWriteAheadSink<IN> extends GenericWriteAheadSink<IN> {
	private static final long serialVersionUID = 999L;
	private final HBaseMapper<IN> hbMapper;
	private String tableOwner;
	private final String tableName;
	private Map<String, Object> hbConfig;
	private transient HBaseClient client;
	private MutationActions mutActions;
	private final boolean writeToWAL = true;
	private CheckpointCommitter committer;
	private boolean logRowkeyNullOnly = false;
	/**
	 *
	 * @param committer Commite complete checkpoint to {@link CheckpointCommitter}
	 * @param serializer Serializer record to StateBackend
	 * @param jobID Job id
	 * @param hbMapper HBase table and input fields {@link HBaseMapper}
	 * @param tName HBase table name
	 * @param tOwer HBase table owner
	 * @param hbConfig HBase user config
	 * @param logRowkeyNullOnly RowKey is null only log not throw exception.
	 * @throws Exception
	 */
	public HBaseWriteAheadSink(CheckpointCommitter committer, TypeSerializer<IN> serializer, String jobID, HBaseMapper<IN> hbMapper, String tName, String tOwer, Map<String, Object> hbConfig, boolean logRowkeyNullOnly) throws Exception {
		super(committer, serializer, jobID);
		Preconditions.checkArgument(tName != null && !tName.isEmpty(), "Table name cannot be null or empty.");
		HBaseSink.checkHBaseMapper(hbMapper);
		this.hbMapper = hbMapper;
		this.tableName = tName;
		this.tableOwner = tOwer;
		this.hbConfig = hbConfig;
		this.mutActions = new MutationActions(new MutationActions.ProcessMode(false, 0, 0, 0));
		this.committer = committer;
		this.logRowkeyNullOnly = logRowkeyNullOnly;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		((HBaseCommitter)committer).setFirstCompleteCheckpointForceStart(!context.isRestored());
	}

	@Override
	public void open() throws Exception {
		// HBase CheckpointCommitter configuration
		((HBaseCommitter)committer).setWriteAheadSink(this);
		super.open();
		try {
			this.client = new HBaseClient(HBaseConfiguration.create(), hbConfig, tableOwner);
			client.connect(tableName);
			mutActions.open(client, writeToWAL);
		} catch (IOException e) {
			LOG.error("HBase base sink preparation failed.", e);
			throw e;
		}
	}

	@Override
	protected boolean sendValues(Iterable<IN> values, long checkpointId, long timestamp) throws Exception {
		// check asynchronous exception
		checkErroneous();
		// record pending sent records
		mutActions.incrementPendingRecords();
		// mapper fields to Hbase table
		outputFormat(values, mutActions, logRowkeyNullOnly);
		// trigger mutation send
		mutActions.sendActions(client, writeToWAL);
		return true;
	}

	private void outputFormat (Iterable<IN> values,
							   MutationActions mutActions,
							   boolean logRowkeyNullOnly) throws Exception {
		for (IN v : values) {
			byte[] rk = this.hbMapper.getRowKey(v);
			if (rk == null || rk.length == 0) {
				if (logRowkeyNullOnly) {
					LOG.info("HBase WAL Sink get rowKey form " +
						"input value is null we discard this value.");
				}else {
					throw new IllegalArgumentException(
						"HBase WAL Sink get rowKey form input value is null or empty.");
				}
			}
			List<Tuple3<byte[], byte[], byte[]>> cl = this.hbMapper.getColumnInfo(v);
			if (cl != null && cl.size() > 0) {
				for (Tuple3<byte[], byte[], byte[]> t : cl) {
					mutActions.addPut(rk, t.f0, t.f1, t.f2);
				}
			}
		}
	}
	private void checkErroneous() throws Exception {
		mutActions.checkErroneous();
	}
}
