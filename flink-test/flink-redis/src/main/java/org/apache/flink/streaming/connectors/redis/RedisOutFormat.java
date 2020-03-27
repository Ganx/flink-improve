package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * 应用模块名称.
 * <p>
 * 代码描述
 * </P>
 *
 * @author xx
 * @since 2020/3/24 18:47
 */
public abstract class RedisOutFormat<IN> extends RichOutputFormat<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisOutFormat.class);

    private FlinkJedisConfigBase flinkJedisConfigBase;

    private RedisCommandsContainer redisCommandsContainer;

    /**
     * Creates a new {@link RedisSinkBase} that connects to the Redis server.
     *
     * @param flinkJedisConfigBase The configuration of {@link FlinkJedisConfigBase}
     */
    public RedisOutFormat(FlinkJedisConfigBase flinkJedisConfigBase) {
        Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        this.flinkJedisConfigBase = flinkJedisConfigBase;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
        }
    }

    @Override
    public void writeRecord(IN o) {
        try {
            writeRecord(o, redisCommandsContainer);
        } catch (Exception e) {
            LOG.error("Redis write data error: ", e);
        }
    }

    public abstract void writeRecord(IN input, RedisCommandsContainer redisCommandsContainer) throws Exception;

    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }
}
