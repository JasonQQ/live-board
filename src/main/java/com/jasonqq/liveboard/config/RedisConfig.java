package com.jasonqq.liveboard.config;


import java.net.InetSocketAddress;
import java.util.Set;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class RedisConfig {

    @Value("${spring.redis.database.cluster.nodes}")
    private Set<InetSocketAddress> nodes;
}
