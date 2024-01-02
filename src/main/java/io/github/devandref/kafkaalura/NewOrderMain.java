package io.github.devandref.kafkaalura;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()) {
            for (var i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var value = key + LocalDate.now();
                var email = key + "@gmail.com";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
