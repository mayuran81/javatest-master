package com.thisisnoble.javatest.events;

import com.thisisnoble.javatest.util.IdGenerator;
import com.thisisnoble.javatest.Event;

public class MarginEvent implements Event {

    private final String id;
    private final String parentId;
    private final double margin;

    public MarginEvent(Event event) {
        this.id = IdGenerator.generate();
        if(event instanceof TradeEvent) {
            TradeEvent te = (TradeEvent) event;
            this.parentId = te.getId();
            this.margin = calculateTradeMargin(te);
        }
        else if (event instanceof ShippingEvent) {
            ShippingEvent se = (ShippingEvent) event;
            this.parentId = se.getId();
            this.margin = calculateShippingMargin(se);
        }
        else {
            throw new IllegalArgumentException("unknown event for margin calculation");
        }
    }

    private double calculateShippingMargin(ShippingEvent se) {
        return se.getShippingCost() * 0.1;
    }

    private double calculateTradeMargin(TradeEvent te) {
        return te.getNotional() * 0.1;
    }

    public String getId() {
        return id;
    }

    public String getParentId() {
        return parentId;
    }

    public double getMargin() {
        return margin;
    }
}
