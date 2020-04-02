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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * This class wraps different HBase sink implementations to provide a common interface for all of them.
 *
 * @param <IN> Type of the elements to be emitted
 */
public class HBaseSink<IN> {
	private final boolean useDataStreamSink;
	private DataStreamSink<IN> sink1;
	private SingleOutputStreamOperator<IN> sink2;
	private HBaseSink(DataStreamSink<IN> sink) {
		sink1 = sink;
		useDataStreamSink = true;
	}
	private HBaseSink(SingleOutputStreamOperator<IN> sink) {
		sink2 = sink;
		useDataStreamSink = false;
	}
	private SinkTransformation<IN> getSinkTransformation() {
		return sink1.getTransformation();
	}
	private StreamTransformation<IN> getStreamTransformation() {
		return sink2.getTransformation();
	}
	/**
	 * Sets the name of this sink. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return The named sink.
	 */
	public HBaseSink<IN> name(String name) {
		if (useDataStreamSink) {
			getSinkTransformation().setName(name);
		} else {
			getStreamTransformation().setName(name);
		}
		return this;
	}
	/**
	 * Sets an ID for this operator.
	 *
	 * <p>The specified ID is used to assign the same operator ID across job
	 * submissions (for example when starting a job from a savepoint).
	 *
	 * <p><strong>Important</strong>: this ID needs to be unique per
	 * transformation and job. Otherwise, job submission will fail.
	 *
	 * @param uid The unique user-specified ID of this transformation.
	 * @return The operator with the specified ID.
	 */
	@PublicEvolving
	public HBaseSink<IN> uid(String uid) {
		if (useDataStreamSink) {
			getSinkTransformation().setUid(uid);
		} else {
			getStreamTransformation().setUid(uid);
		}
		return this;
	}
	/**
	 * Sets an user provided hash for this operator. This will be used AS IS the create the JobVertexID.
	 *
	 * <p>The user provided hash is an alternative to the generated hashes, that is considered when identifying an
	 * operator through the default hash mechanics fails (e.g. because of changes between Flink versions).
	 *
	 * <p><strong>Important</strong>: this should be used as a workaround or for trouble shooting. The provided hash
	 * needs to be unique per transformation and job. Otherwise, job submission will fail. Furthermore, you cannot
	 * assign user-specified hash to intermediate nodes in an operator chain and trying so will let your job fail.
	 *
	 * <p>A use case for this is in migration between Flink versions or changing the jobs in a way that changes the
	 * automatically generated hashes. In this case, providing the previous hashes directly through this method (e.g.
	 * obtained from old logs) can help to reestablish a lost mapping from states to their target operator.
	 *
	 * @param uidHash The user provided hash for this operator. This will become the JobVertexID, which is shown in the
	 *                 logs and web ui.
	 * @return The operator with the user provided hash.
	 */
	@PublicEvolving
	public HBaseSink<IN> setUidHash(String uidHash) {
		if (useDataStreamSink) {
			getSinkTransformation().setUidHash(uidHash);
		} else {
			getStreamTransformation().setUidHash(uidHash);
		}
		return this;
	}
	/**
	 * Sets the parallelism for this sink. The degree must be higher than zero.
	 *
	 * @param parallelism The parallelism for this sink.
	 * @return The sink with set parallelism.
	 */
	public HBaseSink<IN> setParallelism(int parallelism) {
		if (useDataStreamSink) {
			getSinkTransformation().setParallelism(parallelism);
		} else {
			getStreamTransformation().setParallelism(parallelism);
		}
		return this;
	}
	/**
	 * Turns off chaining for this operator so thread co-location will not be
	 * used as an optimization.
	 * <p/>
	 * <p/>
	 * Chaining can be turned off for the whole
	 * job by {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
	 * however it is not advised for performance considerations.
	 *
	 * @return The sink with chaining disabled
	 */
	public HBaseSink<IN> disableChaining() {
		if (useDataStreamSink) {
			getSinkTransformation().setChainingStrategy(ChainingStrategy.NEVER);
		} else {
			getStreamTransformation().setChainingStrategy(ChainingStrategy.NEVER);
		}
		return this;
	}
	/**
	 * Sets the slot sharing group of this operation. Parallel instances of
	 * operations that are in the same slot sharing group will be co-located in the same
	 * TaskManager slot, if possible.
	 *
	 * <p>Operations inherit the slot sharing group of input operations if all input operations
	 * are in the same slot sharing group and no slot sharing group was explicitly specified.
	 *
	 * <p>Initially an operation is in the default slot sharing group. An operation can be put into
	 * the default group explicitly by setting the slot sharing group to {@code "default"}.
	 *
	 * @param slotSharingGroup The slot sharing group name.
	 */
	public HBaseSink<IN> slotSharingGroup(String slotSharingGroup) {
		if (useDataStreamSink) {
			getSinkTransformation().setSlotSharingGroup(slotSharingGroup);
		} else {
			getStreamTransformation().setSlotSharingGroup(slotSharingGroup);
		}
		return this;
	}
	/**
	 * Writes a DataStream into a HBase database.
	 *
	 * @param input input DataStream
	 * @param <IN>  input type
	 * @return HBaseSinkBuilder, to further configure the sink
	 */
	public static <IN> HBaseSinkBuilder<IN> addSink(org.apache.flink.streaming.api.scala.DataStream<IN> input) {
		return addSink(input.javaStream());
	}
	/**
	 * Writes a DataStream into a HBase database.
	 *
	 * @param input input DataStream
	 * @param <IN>  input type
	 * @return HBaseSinkBuilder, to further configure the sink
	 */
	public static <IN> HBaseSinkBuilder<IN> addSink(DataStream<IN> input) {
		return new HBaseSinkBuilder<>(input, input.getType(), input.getType().createSerializer(input.getExecutionEnvironment().getConfig()));
	}

	public static void checkHBaseMapper(HBaseMapper hbMapper) {
		Preconditions.checkArgument(hbMapper != null,
			"HBase mapper cannot be null.");
		Preconditions.checkArgument(hbMapper.rkMapper != null
			&& !hbMapper.rkMapper.isEmpty(),
			"HBase mapper has not set rowkey mapping. Please check it.");
		Preconditions.checkArgument(hbMapper.cfMapper != null
				&& !hbMapper.cfMapper.isEmpty(),
			"HBase mapper has not set column mapping. Please check it.");
	}
	/**
	 * Builder for a {@link HBaseSink}.
	 * @param <IN>
	 */
	public static class HBaseSinkBuilder<IN> {
		private final DataStream<IN> input;
		private final TypeSerializer<IN> serializer;
		private final TypeInformation<IN> typeInfo;
		private String tableName;
		private String tableOwner;
		private MutationActions.ProcessMode processMode;
		private Map<String, Object> hbConfig;
		private HBaseMapper<IN> hbMapper;
		private CheckpointCommitter committer;
		private boolean isWriteAheadLogEnabled;
		private boolean logRowkeyNullOnly = false;
		private HBaseSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			this.input = input;
			this.typeInfo = typeInfo;
			this.serializer = serializer;
		}
		/**
		 * Whether to throw an exception or log msg when rowKey is null.
		 *
		 * @param logRowkeyNullOnly RowKey is null only log not throw exception
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setLogRowkeyNullOnly(boolean logRowkeyNullOnly) {
			this.logRowkeyNullOnly = logRowkeyNullOnly;
			return this;
		}
		/**
		 * Sets the table owner of HBase.
		 *
		 * @param tableOwner table owner.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setTableOwner(String tableOwner) {
			this.tableOwner = tableOwner;
			return this;
		}
		/**
		 * Sets the table owner of HBase.
		 *
		 * @param processMode table owner.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setProcessMode(MutationActions.ProcessMode processMode) {
			this.processMode = processMode;
			return this;
		}
		/**
		 * Sets the HBaseMapper that mapping of input fields to HBase table.
		 *
		 * @param hbMapper map input fields to HBase table.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setHBaseMapper(HBaseMapper<IN> hbMapper) {
			HBaseSink.checkHBaseMapper(hbMapper);
			this.hbMapper = hbMapper;
			return this;
		}
		/**
		 * Sets the HBase connection configuration.
		 *
		 * @param hbConfig HBase connection configuration
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setHBaseConfig(Map<String, Object> hbConfig) {
			this.hbConfig = hbConfig;
			return this;
		}
		/**
		 * Sets the table name of HBase.
		 *
		 * @param tableName table name.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}
		/**
		 * Enables the write-ahead log, which allows exactly-once processing for non-deterministic algorithms that use
		 * idempotent updates.
		 *
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> enableWriteAheadLog() {
			this.isWriteAheadLogEnabled = true;
			return this;
		}
		/**
		 * Enables the write-ahead log, which allows exactly-once processing for non-deterministic algorithms that use
		 * idempotent updates.
		 *
		 * @param committer CheckpointCommitter, that stores information about completed checkpoints in an external
		 *                  resource. By default this information is stored within a separate table within HBase.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> enableWriteAheadLog(CheckpointCommitter committer) {
			this.isWriteAheadLogEnabled = true;
			this.committer = committer;
			return this;
		}
		/**
		 * Finalizes the configuration of this sink.
		 *
		 * @return finalized sink
		 * @throws Exception the exception of building hbase sink
		 */
		public HBaseSink<IN> build() throws Exception {
			sanityCheck();
			return isWriteAheadLogEnabled
				? createWriteAheadSink()
				: createSink();
		}
		private HBaseSink<IN> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(
				new HBaseSinkBase<>(
					tableName,
					tableOwner,
					hbConfig,
					processMode,
					hbMapper))
				.name("HBase Sink"));
		}
		private HBaseSink<IN> createWriteAheadSink() throws Exception {
			Preconditions.checkArgument(
				input.getExecutionEnvironment()
				.getCheckpointConfig()
				.isCheckpointingEnabled(),
				"HBase WAL Sink must be " +
					"enable checkpoint. Please check it.");
			return committer == null
				? new HBaseSink<>(input.transform("HBase WAL Sink", null,
				new HBaseWriteAheadSink<>(
					new HBaseCommitter(),
					serializer, null,
					hbMapper,
					tableName,
					tableOwner,
					hbConfig,
					logRowkeyNullOnly)))
				: new HBaseSink<>(input.transform("HBase WAL Sink", null,
				new HBaseWriteAheadSink<>(committer,
					serializer,
					null,
					hbMapper,
					tableName,
					tableOwner,
					hbConfig,
					logRowkeyNullOnly)));
		}
		private void sanityCheck() {
			Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
				"Table name cannot be null or empty.");
			Preconditions.checkArgument(hbConfig != null,
				"HBase config cannot be null.");
		}
	}
}
