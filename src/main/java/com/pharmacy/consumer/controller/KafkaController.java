package com.pharmacy.consumer.controller;

import com.pharmacy.consumer.producer.DynamicProducer;
import com.pharmacy.consumer.service.GlobalConsumerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Queue;

@RestController
@RequestMapping("/kafka")
@Tag(name = "Kafka Controller", description = "Producer and Consumer APIs with Manual Offset Commits")
public class KafkaController {

    private final DynamicProducer producer;
    private final GlobalConsumerService consumer;

    public KafkaController(DynamicProducer producer, GlobalConsumerService consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    // ✅ PRODUCER ENDPOINTS
    @Operation(summary = "Publish message to Kafka topic with Patient ID as partition key")
    @PostMapping("/produce/{topic}")
    public String publish(@PathVariable String topic,
                          @RequestParam String patientIdAskey,
                          @RequestBody String message) {
        producer.sendMessage(topic, patientIdAskey, message);
        return "✅ Message sent to topic: " + topic + " with key (patientId): " + patientIdAskey;
    }

    // ✅ CONSUMER ENDPOINTS

    /**
     * ✅ Get all consumed messages for a topic (persistent - doesn't clear)
     * Messages here have ALREADY BEEN OFFSET COMMITTED
     * If you restart the consumer, these won't be reprocessed
     */
    @Operation(summary = "Get all consumed messages for a topic (persistent storage)")
    @GetMapping("/consume/{topic}/all")
    public Queue<String> getAllMessagesByTopic(@PathVariable String topic) {
        return consumer.getMessagesByTopic(topic);
    }

    /**
     * ✅ Get last consumed message for a topic
     */
    @Operation(summary = "Get last consumed message for a topic")
    @GetMapping("/consume/{topic}/last")
    public String getLastMessageByTopic(@PathVariable String topic) {
        return consumer.getLastMessageByTopic(topic);
    }

    /**
     * ✅ Get all messages and CLEAR the queue
     * Use this if you want to retrieve messages only once per call
     * The offsets are still committed, data won't be reprocessed
     */
    @Operation(summary = "Get all messages for a topic and clear from memory")
    @GetMapping("/consume/{topic}/all-and-clear")
    public Queue<String> getAndClearMessagesByTopic(@PathVariable String topic) {
        return consumer.getAndClearMessagesByTopic(topic);
    }

    /**
     * ✅ Get offset information per partition
     * Shows which offset we've last committed for each partition
     */
    @Operation(summary = "Get offset information for a topic partition")
    @GetMapping("/consume/{topic}/partition/{partition}/offset")
    public String getOffsetInfo(@PathVariable String topic, @PathVariable int partition) {
        return consumer.getOffsetInfo(topic, partition);
    }

    /**
     * ✅ Get all offset information across all partitions
     */
    @Operation(summary = "Get all offset information across all partitions")
    @GetMapping("/consume/offsets/all")
    public Map<String, String> getAllOffsetInfo() {
        return consumer.getAllOffsetInfo();
    }

    /**
     * ✅ Get message count for a topic
     */
    @Operation(summary = "Get message count for a topic")
    @GetMapping("/consume/{topic}/count")
    public Map<String, Object> getMessageCount(@PathVariable String topic) {
        return Map.of(
            "topic", topic,
            "messageCount", consumer.getMessageCountByTopic(topic)
        );
    }

    /**
     * ✅ Clear all in-memory messages (for testing/debugging)
     * ⚠️ NOTE: This clears in-memory storage BUT NOT Kafka offsets
     * Offsets are committed separately and managed by Kafka
     */
    @Operation(summary = "Clear all in-memory messages (for testing only)")
    @PostMapping("/consume/clear-all")
    public String clearAllMessages() {
        consumer.clearAllMessages();
        return "✅ All in-memory messages cleared. Note: Kafka offsets remain committed.";
    }
}
