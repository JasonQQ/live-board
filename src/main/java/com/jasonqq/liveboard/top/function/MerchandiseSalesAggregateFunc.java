package com.jasonqq.liveboard.top.function;

import com.jasonqq.liveboard.domain.SubOrderDetail;
import org.apache.flink.api.common.functions.AggregateFunction;

public final class MerchandiseSalesAggregateFunc
        implements AggregateFunction<SubOrderDetail, Long, Long> {

    private static final long serialVersionUID = 1L;

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(SubOrderDetail value, Long acc) {
        return acc + value.getQuantity();
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}
