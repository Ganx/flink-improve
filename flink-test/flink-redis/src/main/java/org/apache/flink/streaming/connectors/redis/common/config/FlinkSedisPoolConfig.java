package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.flink.util.Preconditions;

public class FlinkSedisPoolConfig extends FlinkJedisConfigBase{

    private static final long serialVersionUID = 117L;

    private boolean isSharded;
    private int ttlSeconds;
    private String sedisConfigPath;

    /**
     *
     * @param sedisConfigPath Sedis config path
     * @param isSharded is sharded
     * @param ttlSeconds value seconds ttl
     */
    private FlinkSedisPoolConfig(String sedisConfigPath, boolean isSharded, int ttlSeconds) {
        super(1, 1, 1, 1, 0);
        Preconditions.checkArgument(sedisConfigPath != null, "Sedis config path can not be empty.");
        this.sedisConfigPath = sedisConfigPath;
        this.isSharded = isSharded;
        this.ttlSeconds = ttlSeconds;
    }

    /**
     *
     * @return is sharded
     */
    public boolean isSharded() {
        return isSharded;
    }

    /**
     *
     * @return sedis config path
     */
    public String getSedisConfigPath() {
        return sedisConfigPath;
    }

    /**
     *
     * @return value seconds ttl
     */
    public int getTtlSeconds() {
        return ttlSeconds;
    }

    /**
     * Builder for initializing  {@link FlinkSedisPoolConfig}.
     */
    public static class Builder {
        private boolean isSharded;
        private int ttlSeconds;
        private String sedisConfigPath;

        /**
         * Builder constructor
         *
         * @param sedisConfigPath sedis config path
         */
        public Builder(String sedisConfigPath) {
            Preconditions.checkArgument(sedisConfigPath != null, "Sedis config path can not be empty.");
            this.sedisConfigPath = sedisConfigPath;
            this.isSharded = false;
            this.ttlSeconds = -1;
        }

        /**
         * Builder constructor
         *
         * @param sedisConfigPath sedis config path
         * @param isSharded is sharded
         */
        public Builder(String sedisConfigPath,  boolean isSharded) {
            Preconditions.checkArgument(sedisConfigPath != null, "Sedis config path can not be empty.");
            this.sedisConfigPath = sedisConfigPath;
            this.isSharded = isSharded;
            this.ttlSeconds = -1;
        }

        /**
         * Builder constructor
         *
         * @param sedisConfigPath sedis config path
         * @param isSharded is sharded
         * @param ttlSeconds value seconds ttl
         */
        public Builder(String sedisConfigPath,  boolean isSharded, int ttlSeconds) {
            Preconditions.checkArgument(sedisConfigPath != null, "Sedis config path can not be empty.");
            Preconditions.checkArgument(ttlSeconds > 0, "Sedis config seconds ttl must be more than zero.");
            this.sedisConfigPath = sedisConfigPath;
            this.isSharded = isSharded;
            this.ttlSeconds = ttlSeconds;
        }

        /**
         *
         * @param sharded is sharded
         * @return Pool config builer
         */
        public Builder setSharded(boolean sharded) {
            isSharded = sharded;
            return this;
        }

        /**
         *
         * @param sedisConfigPath Sedis config path
         * @return Pool config builer
         */
        public Builder setSedisConfigPath(String sedisConfigPath) {
            this.sedisConfigPath = sedisConfigPath;
            return this;
        }

        /**
         *
         * @param ttlSeconds value seconds ttl
         * @return Pool config builer
         */
        public Builder setTtlSeconds(int ttlSeconds) {
            Preconditions.checkArgument(ttlSeconds > 0, "Sedis config seconds ttl must be more than zero.");
            this.ttlSeconds = ttlSeconds;
            return this;
        }

        /**
         * Builds SedisPoolConfig.
         *
         * @return SedisPoolConfig
         */
        public FlinkSedisPoolConfig build (){
            return new FlinkSedisPoolConfig(sedisConfigPath, isSharded, ttlSeconds);
        }
    }
}
