package com.thisisnoble.javatest.processors;

import com.thisisnoble.javatest.events.RiskEvent;
import com.thisisnoble.javatest.events.TradeEvent;
import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.Orchestrator;

public class RiskProcessor extends AbstractProcessor {

    public RiskProcessor(Orchestrator orchestrator) {
        super(orchestrator);
    }

    @Override
    public boolean interestedIn(Event event) {
        return (event instanceof TradeEvent);
    }

    @Override
    protected Event processInternal(Event event) {
        return new RiskEvent(event);
    }
}
