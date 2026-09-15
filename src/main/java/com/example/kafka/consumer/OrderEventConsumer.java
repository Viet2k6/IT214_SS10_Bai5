package com.example.kafka.consumer;

import com.example.kafka.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(topics = "order-tracking", groupId = "order-group", concurrency = "5")
    public void consumeOrderEvent(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        
        log.info("Nhận sự kiện: {} | Order: {} | Partition: {} | Offset: {}", 
                 event.getStatus(), 
                 event.getOrderId(), 
                 record.partition(),
                 record.offset());
    }
}
