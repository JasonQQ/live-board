package com.jasonqq.liveboard.gmv.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jasonqq.liveboard.gmv.OrderAccumulator;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class OutputOrderGmvProcessFunc
        extends KeyedProcessFunction<Tuple, OrderAccumulator, Tuple2<Long, String>> {

    private static final long serialVersionUID = 1L;

    private MapState<Long, OrderAccumulator> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = this.getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "state_site_order_gmv",
                Long.class,
                OrderAccumulator.class)
        );
    }

    @Override
    public void processElement(OrderAccumulator value, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        long key = value.getSiteId();
        OrderAccumulator cachedValue = state.get(key);

        if (cachedValue == null || !value.getSubOrderSum().equals(cachedValue.getSubOrderSum())) {
            ObjectMapper objectMapper = new ObjectMapper();
            String result = objectMapper.writeValueAsString(value);
            out.collect(new Tuple2<>(key, result));
            state.put(key, value);
        }
    }

    @Override
    public void close() throws Exception {
        state.clear();
        super.close();
    }
}
