/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *  This class represents a list of {@link MutationAction}s you will take when writing
 *  an input value of {@link HBaseSinkBase} to a row in a HBase table.
 *  Each {@link MutationAction} can create an HBase {@link Mutation} operation type
 *  including {@link Put}, {@link Increment}, {@link Append} and {@link Delete}.
 */
public class MutationActions implements Serializable {
	private static final long serialVersionUID = 10086L;
	private transient MutationActions.MutationActionProcesser maproc;
	private ProcessMode pMode;
	private int asyncCheckIntervalTimeMs;
	/** The lock of pending mutationAction. */
	public Lock pendingActionsLock;

	MutationActions(ProcessMode pMode) {
		this.pMode = pMode;
	}
	public void open(HBaseClient client, boolean writeToWAL) {
		this.maproc = new MutationActionProcesser();
		this.maproc.open(client, writeToWAL);
		this.pendingActionsLock = new ReentrantLock();
	}
	/**
	 * Send a list of HBase {@link Mutation}s generated from mutation actions to an HBase table and apply to a specific row.
	 *
	 * @param client an {@link HBaseClient} that connects to an HBase table
	 * @param writeToWAL enable WAL
	 */
	public void sendActions(HBaseClient client, boolean writeToWAL) throws Exception {
		if (!pMode.isAsync()) {
			this.maproc.creatMutationAndSend(client, writeToWAL);
		}
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#PUT}. which will
	 * create an HBase {@link Put} operation for a specified row with specified timestamp.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param value column value
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addPut(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value, long timestamp) throws Exception{
		this.maproc.addMutationAction(rowKey, family, qualifier, value, timestamp, 0, MutationAction.Type.PUT);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#PUT}, which will
	 * create an HBase {@link Put} operation for a specified row with no timestamp specified at client side.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param value column value
	 * @return this
	 */
	public MutationActions addPut(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value) throws Exception{
		this.maproc.addMutationAction(rowKey, family, qualifier, value, 0, 0, MutationAction.Type.PUT);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#APPEND}, which will
	 * create an HBase {@link Append} operation for a specified row.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param value column value
	 * @return this
	 */
	public MutationActions addAppend(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value) throws Exception{
		this.maproc.addMutationAction(rowKey, family, qualifier, value, 0, 0, MutationAction.Type.APPEND);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#INCREMENT}, which will
	 * create an HBase {@link Increment} operation for a specified row with the specified amount.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param amount amount to increment by
	 * @return this
	 */
	public MutationActions addIncrement(byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws Exception{
		this.maproc.addMutationAction(rowKey, family, qualifier, null, 0, amount, MutationAction.Type.INCREMENT);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_ROW}, which will
	 * create an HBase {@link Delete} operation that deletes all columns in all families of the specified row with
	 * a timestamp less than or equal to the specified timestamp.
	 *
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addDeleteRow(long timestamp) throws Exception{
		this.maproc.addMutationAction(null, null, null, null, timestamp, 0, MutationAction.Type.DELETE_ROW);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_ROW}, which will
	 * create an HBase {@link Delete} operation that deletes everything associated with the specified row (all versions
	 * of all columns in all families).
	 *
	 * @return this
	 */
	public MutationActions addDeleteRow() throws Exception{
		this.maproc.addMutationAction(null, null, null, null, Long.MAX_VALUE, 0, MutationAction.Type.DELETE_ROW);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_FAMILY},
	 * which will create an HBase {@link Delete} operation that deletes versions of the specified family with
	 * timestamps less than or equal to the specified timestamp.
	 *
	 * @param family family name
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addDeleteFamily(byte[] family, long timestamp) throws Exception{
		this.maproc.addMutationAction(null, family, null, null, timestamp, 0, MutationAction.Type.DELETE_FAMILY);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_FAMILY},
	 * which will create an HBase {@link Delete} operation that deletes all versions of the specified family.
	 *
	 * @param family family name
	 * @return this
	 */
	public MutationActions addDeleteFamily(byte[] family) throws Exception{
		this.maproc.addMutationAction(null, family, null, null, Long.MAX_VALUE, 0, MutationAction.Type.DELETE_FAMILY);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_COLUMNS},
	 * which will create an HBase {@link Delete} operation that deletes versions of the specified column with
	 * timestamps less than or equal to the specified timestamp.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addDeleteColumns(byte[] family, byte[] qualifier, long timestamp) throws Exception{
		this.maproc.addMutationAction(null, family, qualifier, null, timestamp, 0, MutationAction.Type.DELETE_COLUMNS);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_COLUMNS},
	 * which will create an HBase {@link Delete} operation that deletes all versions of the specified column.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @return this
	 */
	public MutationActions addDeleteColumns(byte[] family, byte[] qualifier) throws Exception{
		this.maproc.addMutationAction(null, family, qualifier, null, Long.MAX_VALUE, 0, MutationAction.Type.DELETE_COLUMNS);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_COLUMN},
	 * which will create an HBase {@link Delete} operation that deletes the specified version of the specified column.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addDeleteColumn(byte[] family, byte[] qualifier, long timestamp) throws Exception{
		this.maproc.addMutationAction(null, family, qualifier, null, timestamp, 0, MutationAction.Type.DELETE_COLUMN);
		return this;
	}
	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_COLUMN},
	 * which will create an HBase {@link Delete} operation that deletes the latest version of the specified column.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @return this
	 */
	public MutationActions addDeleteColumn(byte[] family, byte[] qualifier) throws Exception{
		this.maproc.addMutationAction(null, family, qualifier, null, 0, 0, MutationAction.Type.DELETE_COLUMN);
		return this;
	}
	private MutationActions.ProcessMode getDefaultProcessMode () {
		return new MutationActions.ProcessMode(true, 2097152, 5000, 5 * 1000);
	}
	public void incrementPendingRecords () {
		this.maproc.incrementPendingRecords();
	}
	public void checkErroneous() throws Exception {
		this.maproc.checkErroneous();
	}
	public void flushAndWaitUntilComplete (HBaseClient client, boolean writeToWAL) throws Exception {
		this.maproc.flushAndWaitUntilComplete(client, writeToWAL);
	}
	public void close () {
		this.maproc.close();
	}
	private class MutationActionProcesser {
		/** The mutationActions of pending to be transformed into mutations .*/
		private Map<Long, List<MutationAction>> pendingActions;
		/** The mutations of pending to be sent to HBase.*/
		private Map<Long, List<Mutation>> pendingMutations;
		/** The container of mutationAction for batch conversion to mutations.*/
		private List<MutationAction> actions;
		/** The records of pending to be sent.*/
		private AtomicLong pendingRecords;
		// ------------------- Asynchronous mini batch processing arguments ------------------
		/** The batch number of current mini batch processing.*/
		private AtomicLong currentMiniBatchNo;
		/** The set of records corresponding to the batchNo.*/
		private Map<Long, Long> miniBatchRecords;
		/** The records of current mini batch processing.*/
		private AtomicLong currentMiniBatchRecords;
		/** The size of current mini batch processing.*/
		private AtomicLong currentMiniBatchSize;
		/** The last batch of sent timestamps.*/
		private AtomicLong lastSendTimeStamp;
		/** Catch asynchronous process exception.*/
		private Exception asyncException;
		/** Scheduler of asynchronous check whether to send.*/
		private transient ScheduledExecutorService scheduledExecutor;
		private MutationActionProcesser() {
			pendingActions = new ConcurrentHashMap<>();
			pendingMutations = new ConcurrentHashMap<>();
			actions = new ArrayList<>();
			pendingRecords = new AtomicLong(0L);
			currentMiniBatchNo = new AtomicLong(-1L);
			miniBatchRecords = new ConcurrentHashMap<Long, Long>();
			currentMiniBatchRecords = new AtomicLong(0L);
			currentMiniBatchSize = new AtomicLong(0L);
			lastSendTimeStamp = new AtomicLong(0L);
			if (asyncCheckIntervalTimeMs <= 0) {
				asyncCheckIntervalTimeMs = 500;
			}
			if (pMode == null) {
				pMode = getDefaultProcessMode();
			}
		}
		private void open(final HBaseClient client, final boolean writeToWAL) {
			if (pMode.isAsync()) {
				scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
				scheduledExecutor.scheduleAtFixedRate(new Runnable() {
					@Override
					public void run() {
						try {
							asyncCheckAndSend(client, writeToWAL);
						} catch (Exception e) {
							asyncException = e;
						}
					}
				}, asyncCheckIntervalTimeMs, asyncCheckIntervalTimeMs, TimeUnit.MILLISECONDS);
			}
		}
		private void addMutationAction (byte[] rowKey, byte[] family, byte[] qualifier, byte[] value, long timestamp, long amount, MutationAction.Type type) throws Exception{
			MutationAction ma = new MutationAction(rowKey, family, qualifier, value, timestamp, amount, type);
			if (pMode.isAsync()) {
				actions.add(ma);
				currentMiniBatchSize.getAndSet(currentMiniBatchSize.get() + ma.mActionSize());
			}else {
				List<MutationAction> mas = pendingActions.get(currentMiniBatchNo.get());
				if (mas == null) {
					mas = new ArrayList<>();
					mas.add(ma);
					pendingActions.put(currentMiniBatchNo.get(), mas);
				}else {
					mas.add(ma);
				}
			}
		}
		private void asyncCheckAndSend (HBaseClient client, boolean writeToWAL) throws Exception {
			if ((actions != null && actions.size() > 0)
				&& isTriggerSend()) {
				creatMutationAndSend(client, writeToWAL);
			}
		}

		private boolean isTriggerSend() {
			if (pMode.getFlushBatchInterval() > 0 && currentMiniBatchRecords.get() >= pMode.getFlushBatchInterval()) {
				return true;
			}

			if (pMode.getFlushBatchSize() > 0 && currentMiniBatchSize.get() >= pMode.getFlushBatchSize()) {
				return true;
			}

			if (pMode.getFlushTimeIntervalMs() > 0 &&
				lastSendTimeStamp.get() != 0 &&
				System.currentTimeMillis() - lastSendTimeStamp.get() >= pMode.getFlushTimeIntervalMs()) {
				return true;
			}

			return false;
		}

		private void creatMutationAndSend (HBaseClient client, boolean writeToWAL) throws Exception {
			pendingActionsLock.lock();
			try {
				if (pMode.isAsync()) {
					List<MutationAction> mas = pendingActions.get(currentMiniBatchNo.get());
					if (mas == null && actions.size() > 0) {
						mas = new ArrayList<>();
						mas.addAll(actions);
						pendingActions.put(currentMiniBatchNo.get(), mas);
					}
					if (mas != null && actions.size() > 0) {
						mas.addAll(actions);
					}
					actions.clear();
				}

				if (pendingActions.size() > 0) {
					Long cmbr = miniBatchRecords.get(currentMiniBatchNo.get());
					if (cmbr == null) {
						cmbr = currentMiniBatchRecords.getAndSet(0L);
					}else {
						cmbr += currentMiniBatchRecords.getAndSet(0L);
					}
					miniBatchRecords.put(currentMiniBatchNo.get(), cmbr);
					if (pMode.isAsync()) {
						// prepare a new mini batch
						currentMiniBatchNo.incrementAndGet();
						currentMiniBatchSize.getAndSet(0L);
						lastSendTimeStamp.getAndSet(System.currentTimeMillis() + (asyncCheckIntervalTimeMs / 2));
					}
					createMutations(pendingActions, writeToWAL);
					for (Map.Entry<Long, List<Mutation>> me : pendingMutations.entrySet()) {
						client.send(me.getValue());
						clearAlreadyDoneBatch(me.getKey());
					}
				}
			}finally {
				pendingActionsLock.unlock();
			}
		}
		private void createMutations (Map<Long, List<MutationAction>> pendingActions, boolean writeToWAL) {
			Preconditions.checkArgument(pendingActions != null && pendingActions.size() > 0, "Create mutations and pendingActions cannot be empty.");
			for (Map.Entry<Long, List<MutationAction>> mae : pendingActions.entrySet()) {
				List<Mutation> mutations = createMutations(mae.getValue(), writeToWAL);
				List<Mutation> ms = pendingMutations.get(mae.getKey());
				if (ms == null) {
					pendingMutations.put(mae.getKey(), mutations);
				}else {
					ms.addAll(mutations);
				}
			}
		}
		private List<Mutation> createMutations (List<MutationAction> actions, boolean writeToWAL) {
			Preconditions.checkArgument(actions != null && actions.size() > 0, "Create mutations and actions cannot be empty.");
			Durability durability = writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL;
			List<Mutation> mutations = new ArrayList<>();
			for (MutationAction action : actions) {
				byte[] rowKey = action.getRowkey();
				switch (action.getType()) {
					case PUT:
						Put put = new Put(rowKey);
						if (action.getTs() == 0) {
							put.addColumn(action.getFamily(), action.getQualifier(), action.getValue());
						} else {
							put.addColumn(action.getFamily(), action.getQualifier(), action.getTs(), action.getValue());
						}
						put.setDurability(durability);
						mutations.add(put);
						break;
					case INCREMENT:
						Increment increment = new Increment(rowKey);
						increment.addColumn(action.getFamily(), action.getQualifier(), action.getIncrement());
						increment.setDurability(durability);
						mutations.add(increment);
						break;
					case APPEND:
						Append append = new Append(rowKey);
						append.add(action.getFamily(), action.getQualifier(), action.getValue());
						append.setDurability(durability);
						mutations.add(append);
						break;
					case DELETE_ROW:
						Delete deleteR = new Delete(rowKey, action.getTs());
						deleteR.setDurability(durability);
						mutations.add(deleteR);
						break;
					case DELETE_FAMILY:
						Delete deleteF = new Delete(rowKey);
						deleteF.addFamily(action.getFamily(), action.getTs());
						deleteF.setDurability(durability);
						mutations.add(deleteF);
						break;
					case DELETE_COLUMNS:
						Delete deleteCs = new Delete(rowKey);
						deleteCs.addColumns(action.getFamily(), action.getQualifier(), action.getTs());
						deleteCs.setDurability(durability);
						mutations.add(deleteCs);
						break;
					case DELETE_COLUMN:
						Delete deleteC = new Delete(rowKey);
						if (action.getTs() == 0) {
							deleteC.addColumn(action.getFamily(), action.getQualifier());
						} else {
							deleteC.addColumn(action.getFamily(), action.getQualifier(), action.getTs());
						}
						deleteC.setDurability(durability);
						mutations.add(deleteC);
						break;
					default:
						throw new IllegalArgumentException("Cannot process such action type: " + action.getType());
				}
			}
			return mutations;
		}
		private void flushAndWaitUntilComplete (HBaseClient client, boolean writeToWAL) throws Exception {
			// flush all records to HBase
			creatMutationAndSend(client, writeToWAL);
			// wait all records util complete
			while (this.pendingRecords.get() != 0) {
				Thread.sleep(50L);
				creatMutationAndSend(client, writeToWAL);
			}
		}
		private void incrementPendingRecords () {
			pendingRecords.incrementAndGet();
			currentMiniBatchRecords.incrementAndGet();
		}
		private void clearAlreadyDoneBatch (long batchNo) {
			if (pendingActions != null && pendingActions.containsKey(batchNo)) {
				pendingActions.remove(batchNo);
			}
			if (pendingMutations != null && pendingMutations.containsKey(batchNo)) {
				pendingMutations.remove(batchNo);
			}
			if (miniBatchRecords != null && miniBatchRecords.containsKey(batchNo)) {
				pendingRecords.getAndSet(pendingRecords.get() - miniBatchRecords.get(batchNo));
				miniBatchRecords.remove(batchNo);
			}
		}
		private void checkErroneous() throws Exception {
			Exception e = asyncException;
			if (e != null) {
				// prevent double throwing
				asyncException = null;
				throw new Exception("Asynchronous send message to HBase failed: " + e.getMessage(), e);
			}
		}
		private void close () {
			if (scheduledExecutor != null) {
				ExecutorUtils.gracefulShutdown(pMode.getFlushTimeIntervalMs(), TimeUnit.MILLISECONDS, scheduledExecutor);
			}
		}
	}
	public static class ProcessMode implements Serializable {
		private boolean async;
		private int flushBatchSize;
		private int flushBatchInterval;
		private int flushTimeIntervalMs;

		public ProcessMode (boolean async, int flushBatchSize, int flushBatchInterval, int flushTimeIntervalMs) {
			Preconditions.checkArgument(flushBatchSize >= 0, "HbaseSink ProcessMode's flushBatchSize must be not negative number.");
			Preconditions.checkArgument(flushBatchInterval >= 0, "HbaseSink ProcessMode's flushBatchInterval must be not negative number.");
			Preconditions.checkArgument(flushTimeIntervalMs >= 0, "HbaseSink ProcessMode's flushTimeIntervalMs must be not negative number.");
			this.async = async;
			this.flushBatchSize = flushBatchSize;
			this.flushBatchInterval = flushBatchInterval;
			this.flushTimeIntervalMs = flushTimeIntervalMs;
			if (flushBatchSize == 0 && flushBatchInterval == 0 && flushTimeIntervalMs == 0) {
				this.async = false;
			}
		}

		public void setAsync(boolean async) {
			this.async = async;
		}
		public boolean isAsync() {
			return async;
		}
		public void setFlushBatchSize(int flushBatchSize) {
			Preconditions.checkArgument(flushBatchSize > 0, "HbaseSink ProcessMode's flushBatchSize must be positive number.");
			this.flushBatchSize = flushBatchSize;
		}
		public int getFlushBatchSize() {
			return flushBatchSize;
		}
		public void setFlushBatchInterval(int flushBatchInterval) {
			Preconditions.checkArgument(flushBatchInterval > 0, "HbaseSink ProcessMode's flushBatchInterval must be positive number.");
			this.flushBatchInterval = flushBatchInterval;
		}
		public int getFlushBatchInterval() {
			return flushBatchInterval;
		}
		public void setFlushTimeInterval(int flushTimeIntervalMs) {
			Preconditions.checkArgument(flushTimeIntervalMs > 0, "HbaseSink ProcessMode's flushTimeIntervalMs must be positive number.");
			this.flushTimeIntervalMs = flushTimeIntervalMs;
		}
		public int getFlushTimeIntervalMs() {
			return flushTimeIntervalMs;
		}
	}
	private static class MutationAction {
		enum Type {
			PUT,
			INCREMENT,
			APPEND,
			DELETE_ROW,
			DELETE_FAMILY,
			DELETE_COLUMNS,
			DELETE_COLUMN,
		}
		private final byte[] rowkey;
		private final byte[] family;
		private final byte[] qualifier;
		private final byte[] value;
		private final long ts;
		private final long increment;
		private final Type type;
		MutationAction(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value, long timestamp, long amount, Type type) {
			Preconditions.checkArgument(timestamp >= 0, "Timestamp cannot be negative.");
			this.rowkey = rowKey;
			this.family = family;
			this.qualifier = qualifier;
			this.value = value;
			this.ts = timestamp;
			this.increment = amount;
			this.type = type;
		}
		MutationAction(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value, long amount, Type type) {
			this(rowKey, family, qualifier, value, 0, amount, type);
		}
		byte[] getRowkey() { return rowkey; }
		byte[] getFamily() { return family; }
		byte[] getQualifier() {
			return qualifier;
		}
		byte[] getValue() {
			return value;
		}
		long getTs() { return ts; }
		long getIncrement() {
			return increment;
		}
		Type getType() {
			return type;
		}
		int mActionSize(){
			return (rowkey == null ? 0 : rowkey.length)
				+ (family == null ? 0 : family.length)
				+ (qualifier == null ? 0 : qualifier.length)
				+ (value == null ? 0 : value.length)
				+ (ts == 0 ? 1 : Bytes.toBytes(ts).length)
				+ (increment == 0 ? 1 : Bytes.toBytes(increment).length);
		}
	}
}
