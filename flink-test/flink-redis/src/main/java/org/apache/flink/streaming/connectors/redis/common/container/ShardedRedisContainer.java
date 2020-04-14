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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.Util;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkShardedJedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.JedisURIHelper;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Redis command container if we want to connect to a Redis cluster.
 */
public class ShardedRedisContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ShardedRedisContainer.class);

    private FlinkShardedJedisPoolConfig shardedJedisPoolConfig;
    private ShardedJedisPool shardedJedisPool;
    private ShardedJedis currentShardedJedis;
    private boolean isRelease;

    /**
     * Initialize Redis command container for Redis cluster.
     *
     * @param shardedJedisPoolConfig sharede jedis pool config
     */
    public ShardedRedisContainer(FlinkShardedJedisPoolConfig shardedJedisPoolConfig) {
        Objects.requireNonNull(shardedJedisPoolConfig, "Sharded jedis pool config can not be null");

        this.shardedJedisPoolConfig = shardedJedisPoolConfig;
    }

    @Override
    public void open() throws Exception {
		GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
		genericObjectPoolConfig.setMaxIdle(shardedJedisPoolConfig.getMaxIdle());
		genericObjectPoolConfig.setMaxTotal(shardedJedisPoolConfig.getMaxTotal());
		genericObjectPoolConfig.setMinIdle(shardedJedisPoolConfig.getMinIdle());

		shardedJedisPool = new ShardedJedisPool(genericObjectPoolConfig, getShardInfo(shardedJedisPoolConfig.getShardInfos()));
		isRelease = false;
    }

    private List<JedisShardInfo> getShardInfo(List<FlinkShardedJedisPoolConfig.SinkJedisShardInfo> sinkJedisShardInfos) {
		Util.checkArgument(sinkJedisShardInfos != null && !sinkJedisShardInfos.isEmpty(), "Sharded jedis info list can not be empty.");
		List<JedisShardInfo> jedisShardInfos = new ArrayList<>();
    	for (FlinkShardedJedisPoolConfig.SinkJedisShardInfo sinkJedisShardInfo : sinkJedisShardInfos) {
    		JedisShardInfo jedisShardInfo;
    		if (sinkJedisShardInfo.getName() != null && !"".equals(sinkJedisShardInfo.getName())) {
    			jedisShardInfo = new JedisShardInfo(sinkJedisShardInfo.getHostOrIp(),
					sinkJedisShardInfo.getPort(), sinkJedisShardInfo.getTimeout(), sinkJedisShardInfo.getName());
			}else {
    			jedisShardInfo = new JedisShardInfo(sinkJedisShardInfo.getHostOrIp(),
					sinkJedisShardInfo.getPort(), sinkJedisShardInfo.getTimeout());
			}

    		if (sinkJedisShardInfo.getPassword() != null && !"".equals(sinkJedisShardInfo.getPassword())) {
    			jedisShardInfo.setPassword(sinkJedisShardInfo.getPassword());
			}

			Class<? extends JedisShardInfo> clz = jedisShardInfo.getClass();
			Field declaredField = null;
			try {
				declaredField = clz.getDeclaredField("db");
				declaredField.setAccessible(true);
				declaredField.set(jedisShardInfo, sinkJedisShardInfo.getDb());
			} catch (Exception e) {
				e.printStackTrace();
			}

			jedisShardInfos.add(jedisShardInfo);
		}
		LOG.info("Use {} init sharded jedis.", jedisShardInfos);
    	return jedisShardInfos;
	}

	@Override
	public void hset(String key, String hashField, String value) throws Exception {
		getInstance().hset(key, hashField, value);
	}

	@Override
	public void hmset(String key, Map<String, String> hMap) throws Exception {
		getInstance().hmset(key, hMap);
	}

	@Override
	public void rpush(String key, String... values) throws Exception {
		getInstance().rpush(key, values);
	}

	@Override
	public void lpush(String key, String... values) throws Exception {
		getInstance().lpush(key, values);
	}

	@Override
	public void sadd(String key, String... values) throws Exception {
		getInstance().sadd(key, values);
	}

	@Override
	public void set(String key, String value) throws Exception {
		getInstance().set(key, value);
	}

	@Override
	public void zadd(String key, Double score, String element) throws Exception {
		getInstance().zadd(key, score, element);
	}

	@Override
	public void zadd(String key, Map<String, Double> scoreMembers) throws Exception {
		getInstance().zadd(key, scoreMembers);
	}

	@Override
	public void expire(String key, int seconds) throws Exception {
		getInstance().expire(key, seconds);
	}

	private ShardedJedis getInstance() throws Exception {
    	if (isRelease) {
    		throw new InterruptedException("ShardedRedisContainer was release resource.");
		}
    	if (currentShardedJedis == null) {
			currentShardedJedis = shardedJedisPool.getResource();
		}
    	return currentShardedJedis;
	}

	/**
     * Closes the {@link JedisCluster}.
     */
    @Override
    public void close() throws IOException {
    	if (shardedJedisPool != null) {
			this.shardedJedisPool.destroy();
		}
    	isRelease = true;
    }

}
