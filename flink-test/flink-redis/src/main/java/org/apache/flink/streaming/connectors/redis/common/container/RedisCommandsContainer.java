package org.apache.flink.streaming.connectors.redis.common.container;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * The container for all available Redis commands.
 */
public interface RedisCommandsContainer extends Serializable {
    /**
     * Open the Jedis container.
     *
     * @throws Exception if the instance can not be opened properly
     */
    void open() throws Exception;

    /**
     * Sets field in the hash stored at key to value.
     * If key does not exist, a new key holding a hash is created.
     * If field already exists in the hash, it is overwritten.
     *
     * @param key Hash name
     * @param hashField Hash field
     * @param value Hash value
     */
    void hset(String key, String hashField, String value) throws Exception;

    /**
     * Sets field in the hash stored at key to value.
     * If key does not exist, a new key holding a hash is created.
     * If field already exists in the hash, it is overwritten.
     *
     * @param key Hash name
     * @param hMap value of map set
     */
    void hmset(String key, Map<String, String> hMap) throws Exception;

    /**
     * Insert the specified value at the tail of the list stored at key.
     * If key does not exist, it is created as empty list before performing the push operation.
     *
     * @param key Name of the List
     * @param values  Value of list to be set
     */
    void rpush(String key, String... values) throws Exception;

    /**
     * Insert the specified value at the head of the list stored at key.
     * If key does not exist, it is created as empty list before performing the push operation.
     *
     * @param key Name of the List
     * @param values  Value of list to be set
     */
    void lpush(String key, String... values) throws Exception;

    /**
     * Add the specified member to the set stored at key.
     * Specified members that are already a member of this set are ignored.
     * If key does not exist, a new set is created before adding the specified members.
     *
     * @param key Name of the Set
     * @param values  Value of Set to be set
     */
    void sadd(String key, String... values) throws Exception;

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten,
     * regardless of its type. Any previous time to live associated with the key is
     * discarded on successful SET operation.
     *
     * @param key the key name in which value to be set
     * @param value the value
     */
    void set(String key, String value) throws Exception;

    /**
     * Adds the specified member with the specified scores to the sorted set stored at key.
     *
     * @param key The name of the Sorted Set
     * @param score Score of the element
     * @param element  element to be added
     */
    void zadd(String key, Double score, String element) throws Exception;

    /**
     * Adds the specified member with the specified scores to the sorted set stored at key.
     *
     * @param key The name of the Sorted Set
     * @param scoreMembers elements
     */
    void zadd(String key, Map<String, Double> scoreMembers) throws Exception;

    /**
     * Set ttl for one k-v pair
     *
     * @param key a key need to set ttl
     * @param seconds ttl
     * @throws Exception
     */
    void expire(String key, int seconds) throws Exception;

    /**
     * Close the Jedis container.
     *
     * @throws IOException if the instance can not be closed properly
     */
    void close() throws IOException;
}
