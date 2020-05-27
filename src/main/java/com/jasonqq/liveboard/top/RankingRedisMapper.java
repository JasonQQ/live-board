package com.jasonqq.liveboard.top;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public final class RankingRedisMapper implements RedisMapper<Tuple2<Long, Long>> {

    private static final long serialVersionUID = 1L;
    private static final String ZSET_NAME_PREFIX = "RT:DASHBOARD:RANKING:";

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.ZADD, ZSET_NAME_PREFIX);
    }

    @Override
    public String getKeyFromData(Tuple2<Long, Long> data) {
        return String.valueOf(data.f0);
    }

    @Override
    public String getValueFromData(Tuple2<Long, Long> data) {
        return String.valueOf(data.f1);
    }
}
