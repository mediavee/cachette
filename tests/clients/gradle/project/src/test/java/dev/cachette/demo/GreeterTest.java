package dev.cachette.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class GreeterTest {
    @Test
    void greetsByName() {
        assertEquals("Hello, cache!", Greeter.greet("cache"));
    }
}
