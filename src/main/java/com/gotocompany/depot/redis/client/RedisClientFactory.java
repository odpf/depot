package com.gotocompany.depot.redis.client;


import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.ttl.RedisTTLFactory;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import com.gotocompany.depot.redis.util.RedisSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;

/**
 * Redis client factory.
 */
public class RedisClientFactory {

    private static final String DELIMITER = ",";

    public static RedisClient getClient(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkConfig.getSinkRedisDeploymentType();
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        return RedisSinkDeploymentType.CLUSTER.equals(redisSinkDeploymentType)
                ? getRedisClusterClient(redisTTL, redisSinkConfig, statsDReporter)
                : new RedisStandaloneClient(new Instrumentation(statsDReporter, RedisStandaloneClient.class), redisSinkConfig);
    }


    private static RedisClusterClient getRedisClusterClient(RedisTtl redisTTL, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        String[] redisUrls = redisSinkConfig.getSinkRedisUrls().split(DELIMITER);
        HashSet<HostAndPort> nodes = new HashSet<>();
        try {
            for (String redisUrl : redisUrls) {
                nodes.add(HostAndPort.parseString(StringUtils.trim(redisUrl)));
            }
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(String.format("Invalid url(s) for redis cluster: %s", redisSinkConfig.getSinkRedisUrls()));
        }

        JedisCluster jedisCluster = new JedisCluster(nodes, RedisSinkUtils.getJedisConfig(redisSinkConfig), redisSinkConfig.getSinkRedisMaxAttempts(), new GenericObjectPoolConfig<>());
        return new RedisClusterClient(new Instrumentation(statsDReporter, RedisClusterClient.class), redisTTL, jedisCluster);
    }
}
