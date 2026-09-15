package com.example.kafka.producer;

import com.example.kafka.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String TOPIC = "order-tracking";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(OrderEvent event) {
        kafkaTemplate.send(TOPIC, event.getOrderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Gửi thành công sự kiện [{}] cho Order [{}] vào partition={}",
                                event.getStatus(),
                                event.getOrderId(),
                                result.getRecordMetadata().partition());
                    } else {
                        log.error("Lỗi khi gửi Order [{}]", event.getOrderId(), ex);
                    }
                });
    }
}
