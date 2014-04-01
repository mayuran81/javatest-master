package com.thisisnoble.javatest.util;

import java.util.UUID;

public class IdGenerator {

    public static String generate() {
        return UUID.randomUUID().toString();
    }
}
