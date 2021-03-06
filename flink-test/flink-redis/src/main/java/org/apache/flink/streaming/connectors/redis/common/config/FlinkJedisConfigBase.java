package org.apache.flink.streaming.connectors.redis.common.config;


import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import org.apache.flink.streaming.connectors.redis.common.Util;

import java.io.Serializable;

/**
 * Base class for Flink Redis configuration.
 */
public abstract class FlinkJedisConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final int maxTotal;
    protected final int maxIdle;
    protected final int minIdle;
    protected final int connectionTimeout;
    protected final int db;

    protected FlinkJedisConfigBase(int connectionTimeout, int maxTotal, int maxIdle, int minIdle, int db) {
        Util.checkArgument(connectionTimeout >= 0, "connection timeout can not be negative");
        Util.checkArgument(maxTotal >= 0, "maxTotal value can not be negative");
        Util.checkArgument(maxIdle >= 0, "maxIdle value can not be negative");
        Util.checkArgument(minIdle >= 0, "minIdle value can not be negative");

        this.connectionTimeout = connectionTimeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
        this.db = db;
    }

    protected FlinkJedisConfigBase(int connectionTimeout, int maxTotal, int maxIdle, int minIdle) {
        this(connectionTimeout, maxTotal, maxIdle, minIdle, 0);
    }

    /**
     * Returns timeout.
     *
     * @return connection timeout
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Get the value for the {@code maxTotal} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code maxTotal} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getMaxTotal()
     */
    public int getMaxTotal() {
        return maxTotal;
    }

    /**
     * Get the value for the {@code maxIdle} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code maxIdle} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getMaxIdle()
     */
    public int getMaxIdle() {
        return maxIdle;
    }

    /**
     * Get the value for the {@code minIdle} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code minIdle} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getMinIdle()
     */
    public int getMinIdle() {
        return minIdle;
    }

    public int getDb() {
        return db;
    }
}
