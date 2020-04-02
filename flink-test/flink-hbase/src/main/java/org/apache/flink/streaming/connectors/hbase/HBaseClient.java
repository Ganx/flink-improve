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

import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A client class that serves to create connection and send data to HBase.
 */
public class HBaseClient implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);
	private String tableOwner;
	private Configuration hbConfig;
	private Connection connection;
	private Table table;
	public HBaseClient(Configuration hbConfig, Map<String, Object> userHbMap, String tableOwner) {
		this.hbConfig = hbConfig;
		setUserConfig(userHbMap, hbConfig);
		this.tableOwner = tableOwner;
	}
	public void connect(String tableName) throws IOException {
		if (tableOwner != null && !tableOwner.isEmpty()) {
			User user = User.create(UserGroupInformation.createRemoteUser(tableOwner));
			connection = ConnectionFactory.createConnection(hbConfig, user);
		}else {
			connection = ConnectionFactory.createConnection(hbConfig);
		}
		TableName name = TableName.valueOf(tableName);
		table = connection.getTable(name);
	}
	public List<Object> send(List<? extends Row> mutations) throws IOException, InterruptedException {
		Object[] results = new Object[mutations.size()];
		table.batch(mutations, results);
		return Arrays.asList(results);
	}
	public Configuration getConfig() {
		return this.hbConfig;
	}
	@Override
	public void close() throws IOException {
		try {
			if (table != null) {
				table.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing HBase table.", e);
		}
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing HBase connection.", e);
		}
	}
	public static void setUserConfig (Map<String, Object> hbMap, Configuration hbConfig) {
		if (hbMap != null && hbMap.size() > 0) {
			Iterator<Map.Entry<String, Object>> hbIterator = hbMap.entrySet().iterator();
			while (hbIterator.hasNext()) {
				Map.Entry<String, Object> entry = hbIterator.next();
				if (entry.getValue() != null && entry.getValue() instanceof String) {
					hbConfig.set(entry.getKey(), (String)entry.getValue());
				} else if (entry.getValue() != null && entry.getValue() instanceof Boolean) {
					hbConfig.setBoolean(entry.getKey(), (Boolean)entry.getValue());
				} else if (entry.getValue() != null && entry.getValue() instanceof Double) {
					hbConfig.setDouble(entry.getKey(), (Double)entry.getValue());
				} else if (entry.getValue() != null && entry.getValue() instanceof Long) {
					hbConfig.setLong(entry.getKey(), (Long)entry.getValue());
				} else if (entry.getValue() != null && entry.getValue() instanceof Integer) {
					hbConfig.setLong(entry.getKey(), (Integer)entry.getValue());
				} else if (entry.getValue() != null && entry.getValue() instanceof InetSocketAddress) {
					hbConfig.setSocketAddr(entry.getKey(), (InetSocketAddress)entry.getValue());
				} else if (entry.getValue() != null && entry.getValue() instanceof Pattern) {
					hbConfig.setPattern(entry.getKey(), (Pattern)entry.getValue());
				} else if (entry.getValue() != null && entry.getValue() instanceof String[]) {
					hbConfig.setStrings(entry.getKey(), (String[])entry.getValue());
				} else {
					Preconditions.checkArgument(false, String.format("Please check HbaseConfig key:%s and value:%s type. We only support key type {String} and value type {String,Boolean,Double,Long,InetSocketAddress,Pattern,String[]}.", entry.getKey(), entry.getValue()));
				}
			}
		}
	}
}
