package com.thisisnoble.javatest.processors;

import com.thisisnoble.javatest.events.MarginEvent;
import com.thisisnoble.javatest.events.ShippingEvent;
import com.thisisnoble.javatest.events.TradeEvent;
import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.Orchestrator;

public class MarginProcessor extends AbstractProcessor {

    public MarginProcessor(Orchestrator orchestrator) {
        super(orchestrator);
    }

    @Override
    public boolean interestedIn(Event event) {
        return event instanceof TradeEvent || event instanceof ShippingEvent;
    }

    @Override
    protected Event processInternal(Event event) {
        return new MarginEvent(event);
    }
}
