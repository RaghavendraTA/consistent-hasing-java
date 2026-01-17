package com.raghavendrata.consistenthashing.config;

import com.ecwid.consul.v1.ConsulClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsulClientConfig {

    @Bean
    public ConsulClient consulClient(
            @Value("${spring.cloud.consul.host:localhost}") String host,
            @Value("${spring.cloud.consul.port:8500}") int port) {

        return new ConsulClient(host, port);
    }
}

