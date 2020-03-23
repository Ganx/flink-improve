package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.Util;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Configuration for Jedis cluster.
 */
public class FlinkJedisClusterConfig extends FlinkJedisConfigBase{
    private static final long serialVersionUID = 1L;

    private final Set<InetSocketAddress> nodes;
    private final int maxRedirections;

    /**
     * Jedis cluster configuration.
     * The list of node is mandatory, and when nodes is not set, it throws NullPointerException.
     *
     * @param nodes list of node information for JedisCluster
     * @param connectionTimeout socket / connection timeout. The default is 2000
     * @param maxRedirections limit of redirections-how much we'll follow MOVED or ASK
     * @param maxTotal the maximum number of objects that can be allocated by the pool
     * @param maxIdle the cap on the number of "idle" instances in the pool
     * @param minIdle the minimum number of idle objects to maintain in the pool
     * @throws NullPointerException if parameter {@code nodes} is {@code null}
     */
    private FlinkJedisClusterConfig(Set<InetSocketAddress> nodes, int connectionTimeout, int maxRedirections,
                                    int maxTotal, int maxIdle, int minIdle) {
        super(connectionTimeout, maxTotal, maxIdle, minIdle);
        Objects.requireNonNull(nodes, "Node information should be presented");
        Util.checkArgument(!nodes.isEmpty(), "Redis cluster hosts should not be empty");
        this.nodes = new HashSet<>(nodes);
        this.maxRedirections = maxRedirections;
    }

    /**
     * Returns nodes.
     *
     * @return list of node information
     */
    public Set<HostAndPort> getNodes() {
        Set<HostAndPort> ret = new HashSet<>();
        for (InetSocketAddress node : nodes) {
            ret.add(new HostAndPort(node.getHostName(), node.getPort()));
        }
        return ret;
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
     * Builder for initializing  {@link FlinkJedisClusterConfig}.
     */
    public static class Builder {
        private Set<InetSocketAddress> nodes;
        private int timeout = Protocol.DEFAULT_TIMEOUT;
        private int maxRedirections = 5;
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

        /**
         * Sets list of node.
         *
         * @param nodes list of node
         * @return Builder itself
         */
        public Builder setNodes(Set<InetSocketAddress> nodes) {
            this.nodes = nodes;
            return this;
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
         * Builds JedisClusterConfig.
         *
         * @return JedisClusterConfig
         */
        public FlinkJedisClusterConfig build() {
            return new FlinkJedisClusterConfig(nodes, timeout, maxRedirections, maxTotal, maxIdle, minIdle);
        }
    }

    @Override
    public String toString() {
        return "JedisClusterConfig{" +
                "nodes=" + nodes +
                ", timeout=" + connectionTimeout +
                ", maxRedirections=" + maxRedirections +
                ", maxTotal=" + maxTotal +
                ", maxIdle=" + maxIdle +
                ", minIdle=" + minIdle +
                '}';
    }
}