package service;

import dev.lydtech.dispatch.service.DispatchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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