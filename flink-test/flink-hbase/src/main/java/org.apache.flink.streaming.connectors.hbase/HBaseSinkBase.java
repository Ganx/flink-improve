package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A sink that writes its input to an HBase table.
 * To create this sink you need to pass two arguments the name of the HBase table and {@link HBaseMapper}.
 * A boolean field writeToWAL can also be set to enable or disable Write Ahead Log (WAL).
 * HBase config files must be located in the classpath to create a connection to HBase cluster.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class HBaseSinkBase<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
	private static final long serialVersionUID = 998L;
	private static final Logger LOG = LoggerFactory.getLogger(HBaseSinkBase.class);
	private MutationActions mutActions;
	private transient HBaseClient client;
	private final HBaseMapper<IN> hbMapper;
	private final Map<String, Object> hbConfig;
	private String tableOwner;
	private final String tableName;
	private boolean writeToWAL = true;
	private boolean logRowkeyNullOnly = false;
	/**
	 *
	 * @param tName HBase table name
	 * @param tOwner HBase table owner
	 * @param hbConfig HBase user config
	 * @param pMode HBase sink process {@link MutationActions.ProcessMode}
	 * @param hbMapper HBase table and input fields {@link HBaseMapper}
	 */
	public HBaseSinkBase(String tName,
						 String tOwner,
						 Map<String, Object> hbConfig,
						 MutationActions.ProcessMode pMode,
						 HBaseMapper<IN> hbMapper) {
		this(tName, tOwner, hbConfig, pMode, hbMapper, false);
	}
	/**
	 * The main constructor for creating HBaseSinkBase.
	 *
	 * @param tName HBase table name
	 * @param tOwner HBase table owner
	 * @param hbConfig HBase user config
	 * @param pMode HBase sink process {@link MutationActions.ProcessMode}
	 * @param hbMapper HBase table and input fields {@link HBaseMapper}
	 * @param logRowkeyNullOnly RowKey is null only log not throw exception.
	 */
	public HBaseSinkBase(String tName,
						 String tOwner,
						 Map<String, Object> hbConfig,
						 MutationActions.ProcessMode pMode,
						 HBaseMapper<IN> hbMapper,
						 boolean logRowkeyNullOnly) {
		Preconditions.checkArgument(tName != null && !tName.isEmpty(), "HBaseSinkBase table name cannot be null or empty.");
		Preconditions.checkArgument(hbConfig != null, "HBaseSinkBase hbase config cannot be null.");
		HBaseSink.checkHBaseMapper(hbMapper);
		this.tableName = tName;
		this.tableOwner = tOwner;
		this.hbConfig = hbConfig;
		this.mutActions = new MutationActions(pMode);
		this.hbMapper = hbMapper;
		this.logRowkeyNullOnly = logRowkeyNullOnly;
	}
	/**
	 * Enable or disable WAL when writing to HBase.
	 * Set to true (default) if you want to write {@link Mutation}s to the WAL synchronously.
	 * Set to false if you do not want to write {@link Mutation}s to the WAL.
	 *
	 * @param writeToWAL
	 * @return the HBaseSinkBase with specified writeToWAL value
	 */
	public HBaseSinkBase writeToWAL(boolean writeToWAL) {
		this.writeToWAL = writeToWAL;
		return this;
	}
	@Override
	public void open(Configuration configuration) throws Exception {
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
	public void invoke(IN value, Context context) throws Exception {
		// check asynchronous exception
		checkErroneous();
		// mapper fields to Hbase table
		outputFormat(value, mutActions, logRowkeyNullOnly);
		// trigger mutation send
		mutActions.sendActions(client, writeToWAL);
	}
	private void outputFormat (IN value,
							   MutationActions mutActions,
							   boolean logRowkeyNullOnly) throws Exception {
		byte[] rk = this.hbMapper.getRowKey(value);
		if (rk == null || rk.length == 0) {
			if (logRowkeyNullOnly) {
				LOG.info("HBase Sink get rowKey form " +
					"input value is null we discard this value.");
			}else {
				throw new IllegalArgumentException(
					"HBase Sink get rowKey form input value is null or empty.");
			}
		}
		List<Tuple3<byte[], byte[], byte[]>> cl = this.hbMapper.getColumnInfo(value);
		if (cl != null && cl.size() > 0) {
			mutActions.pendingActionsLock.lock();
			try {
				for (Tuple3<byte[], byte[], byte[]> t : cl) {
					mutActions.addPut(rk, t.f0, t.f1, t.f2);
				}
				// record pending sent records
				mutActions.incrementPendingRecords();
			}finally {
				mutActions.pendingActionsLock.unlock();
			}
		}
	}
	private void checkErroneous() throws Exception {
		mutActions.checkErroneous();
	}
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}
	@Override
	public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
		// check asynchronous exception
		checkErroneous();
		// Waiting for all records to be sent before this checkpoint , guarantee record not lost
		mutActions.flushAndWaitUntilComplete(client, writeToWAL);
	}
	@Override
	public void close() throws Exception {
		client.close();
		mutActions.close();
	}
}
