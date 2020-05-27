package com.jasonqq.liveboard.gmv;

import java.util.HashSet;
import lombok.Data;

@Data
public class OrderAccumulator {

    private Long siteId;
    private String siteName;

    private HashSet<Long> orderIds;

    private Long subOrderSum;

    private Long quantitySum;

    private Long gmv;


    public void addOrderIds(HashSet<Long> orderIds) {
        this.orderIds.addAll(orderIds);
    }

    public void addOrderId(Long orderId) {
        this.orderIds.add(orderId);
    }

    public void addSubOrderSum(Long subOrderSum) {
        this.subOrderSum += subOrderSum;
    }

    public void addQuantitySum(Long quantitySum) {
        this.quantitySum += quantitySum;
    }

    public void addGmv(Long gmv) {
        this.gmv += gmv;
    }
}
