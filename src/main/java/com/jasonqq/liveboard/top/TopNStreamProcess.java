package com.jasonqq.liveboard.top;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jasonqq.liveboard.config.RedisConfig;
import com.jasonqq.liveboard.domain.SubOrderDetail;
import com.jasonqq.liveboard.top.function.MerchandiseSalesAggregateFunc;
import com.jasonqq.liveboard.top.function.MerchandiseSalesWindowFunc;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

@Service
public class TopNStreamProcess {

    private static String ORDER_EXT_TOPIC_NAME = "order";
    private static int PARTITION_COUNT = 1;

    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private KafkaProperties kafkaProperties;

    public void topNJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        Properties consumerProps = new Properties();
        consumerProps.putAll(kafkaProperties.buildConsumerProperties());
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(ORDER_EXT_TOPIC_NAME, new SimpleStringSchema(), consumerProps);
        DataStream<String> sourceStream = env
                .addSource(consumer)
                .setParallelism(PARTITION_COUNT)
                .name("source_kafka_" + ORDER_EXT_TOPIC_NAME)
                .uid("source_kafka_" + ORDER_EXT_TOPIC_NAME);

        ObjectMapper objectMapper = new ObjectMapper();
        DataStream<SubOrderDetail> orderStream = sourceStream
                .map(message -> objectMapper.readValue(message, SubOrderDetail.class))
                .name("map_sub_order_detail").uid("map_sub_order_detail");

        WindowedStream<SubOrderDetail, Tuple, TimeWindow> merchandiseWindowStream = orderStream
                .keyBy("merchandiseId")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)));

        DataStream<Tuple2<Long, Long>> merchandiseRankStream = merchandiseWindowStream
                .aggregate(new MerchandiseSalesAggregateFunc(), new MerchandiseSalesWindowFunc())
                .name("aggregate_merch_sales").uid("aggregate_merch_sales")
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                }));

        FlinkJedisClusterConfig jedisClusterConfig =
                new FlinkJedisClusterConfig.Builder().setNodes(redisConfig.getNodes()).build();
        merchandiseRankStream
                .addSink(new RedisSink<>(jedisClusterConfig, new RankingRedisMapper()))
                .name("sink_redis_top_rank").uid("sink_redis_top_rank")
                .setParallelism(1);

        env.execute("topNJob");
    }

}
