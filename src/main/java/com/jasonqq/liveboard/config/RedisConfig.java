package com.jasonqq.liveboard.config;


import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {

    @Autowired
    RedisProperties redisProperties;

    public Set<InetSocketAddress> getNodes() {
        List<String> nodes = redisProperties.getCluster().getNodes();
        return nodes.stream().map(node -> node.split(":"))
                .map(hostPort -> new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])))
                .collect(Collectors.toSet());
    }
}
