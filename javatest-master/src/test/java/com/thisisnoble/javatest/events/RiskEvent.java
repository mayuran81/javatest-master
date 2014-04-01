package com.thisisnoble.javatest.events;

import com.thisisnoble.javatest.util.IdGenerator;
import com.thisisnoble.javatest.Event;

public class RiskEvent implements Event {

    private final String id;
    private final String parentId;
    private final double riskValue;

    public RiskEvent(Event event) {
        this.id = IdGenerator.generate();
        TradeEvent te = (TradeEvent) event;
        this.parentId = te.getId();
        this.riskValue = calculateRisk(te);
    }

    private double calculateRisk(TradeEvent event) {
        return event.getNotional() * 0.5;
    }

    public String getId() {
        return id;
    }

    public String getParentId() {
        return parentId;
    }

    public double getRiskValue() {
        return riskValue;
    }
}
