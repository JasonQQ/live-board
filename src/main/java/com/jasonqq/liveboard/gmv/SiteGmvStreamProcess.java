package com.jasonqq.liveboard.gmv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jasonqq.liveboard.config.RedisConfig;
import com.jasonqq.liveboard.domain.SubOrderDetail;
import com.jasonqq.liveboard.gmv.function.OrderAndGmvAggregateFunc;
import com.jasonqq.liveboard.gmv.function.OutputOrderGmvProcessFunc;
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
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

public class SiteGmvStreamProcess {

    private static String ORDER_EXT_TOPIC_NAME = "order";
    private static int PARTITION_COUNT = 1;

    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private KafkaProperties kafkaProperties;

    public void siteGMVJob() throws Exception {
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

        WindowedStream<SubOrderDetail, Tuple, TimeWindow> siteDayWindowStream = orderStream
                .keyBy("siteId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));

        DataStream<OrderAccumulator> siteAggStream = siteDayWindowStream
                .aggregate(new OrderAndGmvAggregateFunc())
                .name("aggregate_site_order_gmv").uid("aggregate_site_order_gmv");

        DataStream<Tuple2<Long, String>> siteResultStream = siteAggStream
                .keyBy(0)
                .process(new OutputOrderGmvProcessFunc(), TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {
                }))
                .name("process_site_gmv_changed").uid("process_site_gmv_changed");

        FlinkJedisClusterConfig jedisClusterConfig = new FlinkJedisClusterConfig.Builder().setNodes(redisConfig.getNodes()).build();
        siteResultStream
                .addSink(new RedisSink<>(jedisClusterConfig, new GmvRedisMapper()))
                .name("sink_redis_site_gmv").uid("sink_redis_site_gmv")
                .setParallelism(1);

        env.execute("siteGMVJob");
    }

}
