package com.nazoogomtang.ws.products.service;

import com.nazoogomtang.ws.products.rest.CreateProductRestModel;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class ProductServiceImpl implements ProductService {

    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductRestModel createProductRestModel) {
        String productId = UUID.randomUUID().toString();

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
                createProductRestModel.getTitle(), createProductRestModel.getPrice(),
                createProductRestModel.getQuantity());

        // async
        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate.send("product-created-events-topic",productId,productCreatedEvent);

        future.whenComplete((result, exception) -> {
            if(exception != null){
                log.info("*** Failed to send message: {}", exception.getMessage());
            } else {
                // partition, topic, offset
                log.info("*** Message sent successfully: {}", result.getRecordMetadata());
            }
        });

        log.info("*** Returning product id");
        return productId;
    }
}
