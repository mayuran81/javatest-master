package com.thisisnoble.javatest.impl;

import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;

public class CompositeEvent implements Event {

    private final String id;
    private final Event parent;
    private final List<Event> children = new ArrayList<>();

    public CompositeEvent(Event parent) {
        this.id = IdGenerator.generate();
        this.parent = parent;
    }

    public String getId() {
        return id;
    }

    public Event getParent() {
        return parent;
    }

    public CompositeEvent addChild(Event child) {
        children.add(child);
        return this;
    }

    public Iterable<Event> getChildren() {
        return children;
    }

    public int size() {
        return children.size();
    }
}
