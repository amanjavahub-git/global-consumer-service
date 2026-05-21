package com.pharmacy.consumer.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class DynamicProducer {

    /*private final KafkaTemplate<String, String> kafkaTemplate;

    public DynamicProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, String key, String message) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }
        kafkaTemplate.send(topic, key, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        throw new RuntimeException("Failed to send message: " + ex.getMessage());
                    }
                });
    }*/

    private final KafkaTemplate<String, String> kafkaTemplate;

    public DynamicProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Send pharmacy order message to Kafka
     * @param topic Kafka topic name
     * @param patientId Patient ID (used as key)
     * @param orderJson Pharmacy order JSON payload
     */
    public void sendMessage(String topic, String patientId, String orderJson) {
        kafkaTemplate.send(topic, patientId, orderJson);
        System.out.println("Produced message for PatientId=" + patientId + " to topic=" + topic);
    }
}
