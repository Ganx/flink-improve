package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.Util;
import redis.clients.jedis.Protocol;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class FlinkShardedJedisPoolConfig extends FlinkJedisConfigBase{

    private static final long serialVersionUID = 1L;

    private final List<SinkJedisShardInfo> shardInfos;
    private final int maxRedirections;


    /**
     * Sharded Jedis configuration.
     * The list of node is mandatory, and when nodes is not set, it throws NullPointerException.
     *
     * @param shardInfos list of ip and port information for shard jedis
     * @param connectionTimeout socket / connection timeout. The default is 2000
     * @param maxRedirections limit of redirections-how much we'll follow MOVED or ASK
     * @param maxTotal the maximum number of objects that can be allocated by the pool
     * @param maxIdle the cap on the number of "idle" instances in the pool
     * @param minIdle the minimum number of idle objects to maintain in the pool
     * @throws NullPointerException if parameter {@code nodes} is {@code null}
     */
    private FlinkShardedJedisPoolConfig(List<SinkJedisShardInfo> shardInfos, int connectionTimeout, int maxRedirections,
                                        int maxTotal, int maxIdle, int minIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle);

        Objects.requireNonNull(shardInfos, "ShardInfos information should be presented");
        Util.checkArgument(!shardInfos.isEmpty(), "Redis shard jedis hosts should not be empty");
        this.shardInfos = shardInfos;
        this.maxRedirections = maxRedirections;
    }

    /**
     *
     * @return list of shard jedis ip and port
     */
    public List<SinkJedisShardInfo> getShardInfos() {
        return shardInfos;
    }

    /**
     * Returns limit of redirection.
     *
     * @return limit of redirection
     */
    public int getMaxRedirections() {
        return maxRedirections;
    }


    /**
     * Builder for initializing  {@link FlinkShardedJedisPoolConfig}.
     */
    public static class Builder {
        private List<SinkJedisShardInfo> shardInfos;
        private int timeout = Protocol.DEFAULT_TIMEOUT;
        private int maxRedirections = 5;
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

        /**
         *
         * @param shardInfos list of sharded jedis ip and port
         */
        public void setShardInfos(List<SinkJedisShardInfo> shardInfos) {
            this.shardInfos = shardInfos;
        }

        /**
         * Sets socket / connection timeout.
         *
         * @param timeout socket / connection timeout, default value is 2000
         * @return Builder itself
         */
        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets limit of redirection.
         *
         * @param maxRedirections limit of redirection, default value is 5
         * @return Builder itself
         */
        public Builder setMaxRedirections(int maxRedirections) {
            this.maxRedirections = maxRedirections;
            return this;
        }

        /**
         * Sets value for the {@code maxTotal} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool, default value is 8
         * @return Builder itself
         */
        public Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * Sets value for the {@code maxIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param maxIdle the cap on the number of "idle" instances in the pool, default value is 8
         * @return Builder itself
         */
        public Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        /**
         * Sets value for the {@code minIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param minIdle the minimum number of idle objects to maintain in the pool, default value is 0
         * @return Builder itself
         */
        public Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        /**
         * Builds ShardedJedisPoolConfig.
         *
         * @return ShardedJedisPoolConfig
         */
        public FlinkShardedJedisPoolConfig build() {
            return new FlinkShardedJedisPoolConfig(shardInfos, timeout, maxRedirections, maxTotal, maxIdle, minIdle);
        }
    }

    public static class SinkJedisShardInfo implements Serializable {
        private int timeout;
        private String hostOrIp;
        private int port;
        private String password;
        private String name;

        public String getHostOrIp() {
            return hostOrIp;
        }

        public int getPort() {
            return port;
        }

        public SinkJedisShardInfo(String hostOrIp, String name) {
            this(hostOrIp, Protocol.DEFAULT_PORT, name);
        }

        public SinkJedisShardInfo(String hostOrIp, int port) {
            this(hostOrIp, port, null);
        }

        public SinkJedisShardInfo(String hostOrIp, int port, String name) {
            this(hostOrIp, port, name, null, Protocol.DEFAULT_TIMEOUT);
        }

        public SinkJedisShardInfo(String hostOrIp, int port, String name, String password,int timeout) {
            Util.checkArgument(hostOrIp != null && !"".equals(hostOrIp), "Sharded redis host or ip should not be empty");
            Util.checkArgument(port > 0, "Sharded redis host port must more than zero");
            Util.checkArgument(timeout > 0, "Sharded redis host port must more than zero");
            this.hostOrIp = hostOrIp;
            this.port = port;
            this.name = name;
            this.password = password;
            this.timeout = timeout;
        }

        @Override
        public String toString() {
            return "SinkJedisShardInfo{" +
                    "hostOrIp=" + hostOrIp +
                    ", port=" + port +
                    ", timeout=" + timeout +
                    ", name=" + name +
                    ", password=" + password +
                    "}";
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String auth) {
            this.password = auth;
        }

        public int getTimeout() {
            return timeout;
        }

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

        public String getName() {
            return name;
        }

    }

    @Override
    public String toString() {
        return "ShardedJedisConfig{" +
                "shardInfos=" + shardInfos +
                ", timeout=" + connectionTimeout +
                ", maxRedirections=" + maxRedirections +
                ", maxTotal=" + maxTotal +
                ", maxIdle=" + maxIdle +
                ", minIdle=" + minIdle +
                '}';
    }
}
