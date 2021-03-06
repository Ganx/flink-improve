package org.apache.flink.streaming.connectors.redis.common.container;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.*;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Objects;

public class RedisCommandsContainerBuilder {

    /**
     * Initialize the {@link RedisCommandsContainer} based on the instance type.
     *
     * @param flinkJedisConfigBase configuration base
     * @return @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
    public static RedisCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase) {
        if(flinkJedisConfigBase instanceof FlinkJedisPoolConfig){
            FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisPoolConfig);
        } else if (flinkJedisConfigBase instanceof FlinkSedisPoolConfig) {
            FlinkSedisPoolConfig flinkSedisPoolConfig = (FlinkSedisPoolConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkSedisPoolConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisClusterConfig) {
            FlinkJedisClusterConfig flinkJedisClusterConfig = (FlinkJedisClusterConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisClusterConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisSentinelConfig) {
            FlinkJedisSentinelConfig flinkJedisSentinelConfig = (FlinkJedisSentinelConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisSentinelConfig);
        } else if (flinkJedisConfigBase instanceof FlinkShardedJedisPoolConfig) {
            FlinkShardedJedisPoolConfig flinkShardedJedisPoolConfig = (FlinkShardedJedisPoolConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkShardedJedisPoolConfig);
        }else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    /**
     * Builds container for single Redis environment.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return container for single Redis environment
     * @throws NullPointerException if jedisPoolConfig is null
     */
    public static RedisCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Objects.requireNonNull(jedisPoolConfig, "Redis pool config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());

        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, jedisPoolConfig.getHost(),
                jedisPoolConfig.getPort(), jedisPoolConfig.getConnectionTimeout(), jedisPoolConfig.getPassword(),
                jedisPoolConfig.getDatabase());
        return new RedisContainer(jedisPool);
    }


    /**
     * Builds container for Redis Cluster environment.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @return container for Redis Cluster environment
     * @throws NullPointerException if jedisClusterConfig is null
     */
    public static RedisCommandsContainer build(FlinkJedisClusterConfig jedisClusterConfig) {
        return new RedisClusterContainer(jedisClusterConfig);
    }

    /**
     * Builds container for Redis Sentinel environment.
     *
     * @param jedisSentinelConfig configuration for JedisSentinel
     * @return container for Redis sentinel environment
     * @throws NullPointerException if jedisSentinelConfig is null
     */
    public static RedisCommandsContainer build(FlinkJedisSentinelConfig jedisSentinelConfig) {
        return new RedisContainer(jedisSentinelConfig);
    }

    /**
     * Builds container for single Redis environment.
     *
     * @param shardedJedisPoolConfig configuration for ShaededJedisPool
     * @return container for single Redis environment
     * @throws NullPointerException if jedisPoolConfig is null
     */
    public static RedisCommandsContainer build(FlinkShardedJedisPoolConfig shardedJedisPoolConfig) {
        Objects.requireNonNull(shardedJedisPoolConfig, "Sharded jedis pool config should not be Null");
        return new ShardedRedisContainer(shardedJedisPoolConfig);
    }
}
