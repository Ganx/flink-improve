package org.apache.flink.streaming.connectors.redis.common.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * 应用模块名称.
 * <p>
 * 代码描述
 * </P>
 *
 * @author cj
 * @since 2020/3/23 11:37
 */
public class RedisContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);

    private transient JedisPool jedisPool;
    private transient JedisSentinelPool jedisSentinelPool;
    private transient Jedis currentJedis;
    private boolean isRelease;

    /**
     * Use this constructor if to connect with single Redis server.
     *
     * @param jedisPool JedisPool which actually manages Jedis instances
     */
    public RedisContainer(JedisPool jedisPool) {
        Objects.requireNonNull(jedisPool, "Jedis Pool can not be null");
        this.jedisPool = jedisPool;
        this.jedisSentinelPool = null;
    }

    /**
     * Use this constructor if Redis environment is clustered with sentinels.
     *
     * @param sentinelPool SentinelPool which actually manages Jedis instances
     */
    public RedisContainer(final JedisSentinelPool sentinelPool) {
        Objects.requireNonNull(sentinelPool, "Jedis Sentinel Pool can not be null");
        this.jedisPool = null;
        this.jedisSentinelPool = sentinelPool;
    }

    @Override
    public void open() throws Exception {
// echo() tries to open a connection and echos back the
        // message passed as argument. Here we use it to monitor
        // if we can communicate with the cluster.

        getInstance().echo("Test");
        isRelease = false;
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

    @Override
    public void close() throws IOException {
        releaseInstance();
        if (this.jedisPool != null) {
            this.jedisPool.destroy();
        }
        if (this.jedisSentinelPool != null) {
            this.jedisSentinelPool.destroy();
        }
        isRelease = true;
    }

    /**
     * Returns Jedis instance from the pool.
     *
     * @return the Jedis instance
     */
    private Jedis getInstance() throws Exception {
        if (isRelease) {
            throw new InterruptedException("ShardedRedisContainer was release resource.");
        }
        if (currentJedis == null || !currentJedis.isConnected()) {
            if (jedisSentinelPool != null) {
                currentJedis = jedisSentinelPool.getResource();
            } else {
                currentJedis = jedisPool.getResource();
            }
        }
        return currentJedis;
    }

    /**
     * Closes the jedis instance.
     */
    private void releaseInstance() {
        try {
            if (currentJedis != null) {
                currentJedis.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close (return) instance to pool", e);
        }
    }
}
