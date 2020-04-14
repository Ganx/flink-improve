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
package org.apache.flink.streaming.connectors.redis.common.container;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Redis command container if we want to connect to a Redis cluster.
 */
public class RedisClusterContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterContainer.class);

    private transient JedisCluster jedisCluster;


    public RedisClusterContainer(FlinkJedisClusterConfig jedisClusterConfig){
		Objects.requireNonNull(jedisClusterConfig, "Redis cluster config should not be Null");

		GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
		genericObjectPoolConfig.setMaxIdle(jedisClusterConfig.getMaxIdle());
		genericObjectPoolConfig.setMaxTotal(jedisClusterConfig.getMaxTotal());
		genericObjectPoolConfig.setMinIdle(jedisClusterConfig.getMinIdle());

		JedisCluster jedisCluster = new JedisCluster(jedisClusterConfig.getNodes(),
				jedisClusterConfig.getConnectionTimeout(), jedisClusterConfig.getMaxRedirections());

		Objects.requireNonNull(jedisCluster, "Jedis cluster can not be null");

		jedisCluster.select(jedisClusterConfig.getDb());
		this.jedisCluster = jedisCluster;
	}

    @Override
    public void open() throws Exception {

        // echo() tries to open a connection and echos back the
        // message passed as argument. Here we use it to monitor
        // if we can communicate with the cluster.

        jedisCluster.echo("Test");
    }

	@Override
	public void hset(String key, String hashField, String value) throws Exception {
		jedisCluster.hset(key, hashField, value);
	}

	@Override
	public void hmset(String key, Map<String, String> hMap) throws Exception {
		jedisCluster.hmset(key, hMap);
	}

	@Override
	public void rpush(String key, String... values) throws Exception {
		jedisCluster.rpush(key, values);
	}

	@Override
	public void lpush(String key, String... values) throws Exception {
		jedisCluster.lpush(key, values);
	}

	@Override
	public void sadd(String key, String... values) throws Exception {
		jedisCluster.sadd(key, values);
	}

	@Override
	public void set(String key, String value) throws Exception {
		jedisCluster.set(key, value);
	}

	@Override
	public void zadd(String key, Double score, String element) throws Exception {
		jedisCluster.zadd(key, score, element);
	}

	@Override
	public void zadd(String key, Map<String, Double> scoreMembers) throws Exception {
		jedisCluster.zadd(key, scoreMembers);
	}

	@Override
	public void expire(String key, int seconds) throws Exception {
		jedisCluster.expire(key, seconds);
	}

	/**
     * Closes the {@link JedisCluster}.
     */
    @Override
    public void close() throws IOException {
    	if (jedisCluster != null) {
			this.jedisCluster.shutdown();
		}
    }

}
