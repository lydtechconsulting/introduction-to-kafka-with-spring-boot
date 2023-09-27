package service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DispatchServiceTest {

    private DispatchService service;
    @BeforeEach
    void setUp() {
        service = new DispatchService();
    }
    @Test
    void process() {
        service.process("payload");
    }
}