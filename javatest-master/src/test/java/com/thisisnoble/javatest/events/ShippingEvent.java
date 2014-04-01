package com.thisisnoble.javatest.events;

import com.thisisnoble.javatest.util.IdGenerator;
import com.thisisnoble.javatest.Event;

public class ShippingEvent implements Event {

    private final String id;
    private final String parentId;
    private final double shippingCost;

    public ShippingEvent(String id, double shippingCost) {
        this.id = id;
        this.parentId = null;
        this.shippingCost = shippingCost;
    }

    public ShippingEvent(Event event) {
        this.id = IdGenerator.generate();
        if(event instanceof TradeEvent) {
            TradeEvent te = (TradeEvent) event;
            this.parentId = te.getId();
            this.shippingCost = calculateTradeShipping(te);
        }
        else if (event instanceof ShippingEvent) {
            ShippingEvent se = (ShippingEvent) event;
            this.parentId = se.getId();
            this.shippingCost = calculateShipping(se);
        }
        else {
            throw new IllegalArgumentException("unknown event for margin calculation");
        }
    }

    private double calculateTradeShipping(TradeEvent te) {
        return te.getNotional() * 0.1;
    }

    private double calculateShipping(ShippingEvent se) {
        return se.shippingCost + 10;
    }

    public String getId() {
        return id;
    }

    public String getParentId() {
        return parentId;
    }

    public double getShippingCost() {
        return shippingCost;
    }
}
