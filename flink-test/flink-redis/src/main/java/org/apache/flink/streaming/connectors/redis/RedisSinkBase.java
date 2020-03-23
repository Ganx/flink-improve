package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class RedisSinkBase <IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkBase.class);

    private FlinkJedisConfigBase flinkJedisConfigBase;

    private RedisCommandsContainer redisCommandsContainer;

    /**
     * Creates a new {@link RedisSinkBase} that connects to the Redis server.
     *
     * @param flinkJedisConfigBase The configuration of {@link FlinkJedisConfigBase}
     */
    public RedisSinkBase(FlinkJedisConfigBase flinkJedisConfigBase) {
        Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        this.flinkJedisConfigBase = flinkJedisConfigBase;
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel.
     * Depending on the specified Redis data type (see {@link RedisDataType}),
     * a different Redis command will be applied.
     * Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        invoke(input, context, redisCommandsContainer);
    }

    public abstract void invoke(IN input, Context context, RedisCommandsContainer redisCommandsContainer) throws Exception;

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}
