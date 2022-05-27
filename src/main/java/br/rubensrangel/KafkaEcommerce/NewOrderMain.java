package br.rubensrangel.KafkaEcommerce;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try (var dispatcher = new KafkaDispatcher()) {

            for (var i = 0; i < 10; i++) {

                var key = UUID.randomUUID().toString();
                var value = key + "132,123,124123,124124";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = " Bem vindo! Processando sua compra!";
                var emailRecord = "ECOMMERCE_SEND_EMAIL";
                dispatcher.send(emailRecord, key, email);
            }
        }
    }
}
