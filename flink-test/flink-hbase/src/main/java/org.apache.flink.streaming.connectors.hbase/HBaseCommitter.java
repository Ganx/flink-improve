package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CheckpointCommitter that saves information about completed checkpoints within a separate table in a hbase
 * database.<br>
 *
 * <p>The table structure is as follows
 * <p>| 			                           |         columnFamily           |
 * <p>|               rowKey                  |    completeCheckpointInfo      |
 * <p>| jobUser_jobName_subTaskId_operatorId  |     completeCheckpointId       |
 */
public class HBaseCommitter extends CheckpointCommitter {
	private static final long serialVersionUID = 666L;
	private String jobName;
	private String jobUser;
	private String rowKeyPrefix;
	private String committerTableName;
	private String committerTableOwner;
	private boolean isFirstCompleteCheckpointForceStart;
	private transient HBaseClient client;
	private Map<String, Object> hbConfig;
	private GenericWriteAheadSink writeAheadSink;
	private ExecutionConfig fkConfig;
	/**
	 * A cache of the last committed checkpoint ids per subtask index. This is used to
	 * avoid redundant round-trips to HBase (see {@link #isCheckpointCommitted(int, long)}.
	 */
	private final Map<Integer, Long> lastCommittedCheckpoints = new HashMap<>();
	public HBaseCommitter() {
		this(null, null, null);
	}

	public HBaseCommitter(Map<String, Object> hbConfig, String committerTableName, String committerTableOwner) {
		this(null, null, hbConfig, committerTableName, committerTableOwner);
	}

	public HBaseCommitter(String jobName, String jobUser, Map<String, Object> hbConfig, String committerTableName, String committerTableOwner) {
		this(null, jobName, jobUser, null, hbConfig, committerTableName, committerTableOwner);
	}

	public HBaseCommitter(String jobId, String jobName, String jobUser, String operatorId, Map<String, Object> hbConfig, String committerTableName, String committerTableOwner) {
		this.jobId = jobId;
		this.operatorId = operatorId;
		this.jobName = jobName;
		this.jobUser = jobUser;
		this.hbConfig = hbConfig;
		this.isFirstCompleteCheckpointForceStart = true;
		this.committerTableName = committerTableName;
		this.committerTableOwner = committerTableOwner;
	}


	@Override
	public void open() throws Exception {
		boolean confError = false;
		String confErrorMsg = "";
		setJobName(writeAheadSink.getContainingTask().getEnvironment().getMetricGroup().parent().jobName());
		String hadoopUser = System.getenv().get("HADOOP_USER_NAME");
		setJobUser(hadoopUser !=null && !hadoopUser.isEmpty() ? hadoopUser : "flinkuser");
		StreamOperator[] allOperators = ((OperatorChain) writeAheadSink.getContainingTask().getStreamStatusMaintainer()).getAllOperators();
		for (int chainIdx = 0; chainIdx < allOperators.length; ++chainIdx) {
			if (writeAheadSink == allOperators[chainIdx]) {
				this.operatorId = String.valueOf(chainIdx);
			}
		}
		if (operatorId == null || operatorId.isEmpty()) {
			confError = true;
			confErrorMsg = "HBaseCommitter can not get operatorId.";
		}
		if (hbConfig == null || hbConfig.size() == 0) {
			if (fkConfig.getGlobalJobParameters() == null) {
				throw new IllegalArgumentException("HBaseWriteAheadSink only support single job on yarn please check it.");
			}
			Map<String, String> fkMap = fkConfig.getGlobalJobParameters().toMap();
			hbConfig = new HashMap<>();
			if (fkMap.containsKey(HConstants.ZOOKEEPER_QUORUM)) {
				hbConfig.put(HConstants.ZOOKEEPER_QUORUM, fkMap.get(HConstants.ZOOKEEPER_QUORUM));
			} else {
				confError = true;
				confErrorMsg = "HBaseCommitter hbase zookeeper cannot be empty.Please check flink configuration has configured \"hbase.zookeeper.quorum\"";
			}
			if (fkMap.containsKey(HConstants.ZOOKEEPER_CLIENT_PORT)) {
				hbConfig.put(HConstants.ZOOKEEPER_CLIENT_PORT, fkMap.get(HConstants.ZOOKEEPER_CLIENT_PORT));
			}else {
				confError = true;
				confErrorMsg = "HBaseCommitter hbase zookeeper port cannot be empty.Please check flink configuration has configured \"hbase.zookeeper.property.clientPort\"";
			}
			if (fkMap.containsKey(HConstants.ZOOKEEPER_ZNODE_PARENT)) {
				hbConfig.put(HConstants.ZOOKEEPER_ZNODE_PARENT, fkMap.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
			}else {
				confError = true;
				confErrorMsg = "HBaseCommitter hbase zookeeper parent node cannot be empty.Please check flink configuration has configured \"zookeeper.znode.parent\"";
			}
			if (committerTableOwner == null && fkMap.containsKey("hbase.table.owner")) {
				committerTableOwner = fkMap.get("hbase.table.owner");
			}else {
				LOG.warn("HBaseCommitter initialize hbase.table.owner is null.");
			}
			if (committerTableName == null && fkMap.containsKey("hbase.table.name")) {
				committerTableName = fkMap.get("hbase.table.name");
			}else {
				confError = true;
				confErrorMsg = "HBaseCommitter hbase table cannot be empty.Please check flink configuration has configured \"hbase.table.name\"";
			}
		}
		if (confError) {
			LOG.error(confErrorMsg);
			throw new IllegalArgumentException(confErrorMsg);
		}
		try {
			this.client = new HBaseClient(HBaseConfiguration.create(), hbConfig, committerTableOwner);
			client.connect(committerTableName);
		} catch (IOException e) {
			LOG.error("HBase CheckpointCommitter preparation failed.", e);
			throw e;
		}
		Preconditions.checkArgument(jobName != null && !jobName.isEmpty(), "HBaseCommitter jobName cannot be empty.");
		Preconditions.checkArgument(jobUser != null && !jobUser.isEmpty(), "HBaseCommitter jobUser cannot be empty.");
		Preconditions.checkArgument(operatorId != null && !operatorId.isEmpty(), "HBaseCommitter operatorId cannot be empty.");
		StringBuilder rpb = new StringBuilder();
		rpb.append(jobUser);
		rpb.append("_");
		rpb.append(jobName);
		rpb.append("_");
		rpb.append(operatorId);
		rpb.append("_");
		this.rowKeyPrefix = rpb.toString();
	}

	@Override
	public void close() throws Exception {
		client.close();
	}

	@Override
	public void createResource() throws Exception {
		// Now We Do Nothing
	}

	@Override
	public void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception {
		// build and put completeCheckpoint info to third-party storage system
		byte[] rk = Bytes.toBytes(rowKeyPrefix + subtaskIdx);
		//byte[] cf = Bytes.toBytes("completeCheckpointInfo");
		byte[] cf = Bytes.toBytes("f");
		byte[] cq = Bytes.toBytes("completeCheckpointId");
		byte[] qv = Bytes.toBytes(checkpointID);
		Put put = new Put(rk);
		put.addColumn(cf, cq, qv);
		put.setDurability(Durability.SYNC_WAL);
		client.send(Arrays.asList(put));
		lastCommittedCheckpoints.put(subtaskIdx, checkpointID);
	}

	@Override
	public boolean isCheckpointCommitted(int subtaskIdx, long checkpointId) throws Exception {
		// Pending checkpointed buffers are committed in ascending order of their
		// checkpoint id. This way we can tell if a checkpointed buffer was committed
		// just by asking the third-party storage system for the last checkpoint id
		// committed by the specified subtask.
		Long lastCommittedCheckpoint = lastCommittedCheckpoints.get(subtaskIdx);
		if (lastCommittedCheckpoint == null) {
			if (isFirstCompleteCheckpointForceStart) {
				lastCommittedCheckpoint = -1L;
				isFirstCompleteCheckpointForceStart = false;
			}else {
				byte[] rk = Bytes.toBytes(rowKeyPrefix + subtaskIdx);
				Get get = new Get(rk);
				List<Object> rs = client.send(Arrays.asList(get));
				if (rs != null && rs.size() > 0) {
					Result r = (Result) rs.get(0);
					Cell[] cs = r.rawCells();
					if (cs != null && cs.length > 0) {
						lastCommittedCheckpoint = Bytes.toLong(CellUtil.cloneValue(cs[0]));
					}
				}
			}
		}
		return lastCommittedCheckpoint != null && checkpointId <= lastCommittedCheckpoint;
	}

	@Override
	public void setOperatorId(String id) throws Exception {
		// nothing to do,set HBaseCommitter's operatorId in open function.
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setJobUser(String jobUser) {
		this.jobUser = jobUser;
	}

	public void setFirstCompleteCheckpointForceStart(boolean firstCompleteCheckpointForceStart) {
		this.isFirstCompleteCheckpointForceStart = firstCompleteCheckpointForceStart;
	}

	public void setWriteAheadSink(GenericWriteAheadSink writeAheadSink) {
		this.writeAheadSink = writeAheadSink;
		this.fkConfig = writeAheadSink.getContainingTask().getExecutionConfig();
	}
}
