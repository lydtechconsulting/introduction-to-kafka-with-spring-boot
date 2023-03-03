package dev.lydtech.dispatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// SpringBootApplication is the same as @Configuration, @EnableAutoConfiguration and @ComponentScan, which results in the kafkaProducer getting automatically instantiated
@SpringBootApplication
public class DispatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(DispatchApplication.class, args);
    }
}
