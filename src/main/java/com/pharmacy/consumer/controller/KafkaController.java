package com.pharmacy.consumer.controller;

import com.pharmacy.consumer.service.GlobalConsumerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Queue;

@RestController
@RequestMapping("/kafka")
@Tag(name = "Kafka Controller", description = "Producer and Consumer APIs with Manual Offset Commits and Retry Control")
public class KafkaController {

    private final GlobalConsumerService consumer;

    public KafkaController(GlobalConsumerService consumer) {
        this.consumer = consumer;
    }

    // existing endpoints omitted for brevity; add the new ones below

    @Operation(summary = "Get retry count for a specific message")
    @GetMapping("/consume/{topic}/partition/{partition}/offset/{offset}/retry")
    public Map<String, Object> getRetryCount(@PathVariable String topic,
                                             @PathVariable int partition,
                                             @PathVariable long offset) {
        Integer retries = consumer.getRetryCount(topic, partition, offset);
        return Map.of(
                "topic", topic,
                "partition", partition,
                "offset", offset,
                "retryCount", retries == null ? 0 : retries
        );
    }

    @Operation(summary = "Get skipped info for a specific message")
    @GetMapping("/consume/{topic}/partition/{partition}/offset/{offset}/skipped")
    public Map<String, Object> getSkippedInfo(@PathVariable String topic,
                                              @PathVariable int partition,
                                              @PathVariable long offset) {
        String info = consumer.getSkippedInfo(topic, partition, offset);
        return Map.of(
                "topic", topic,
                "partition", partition,
                "offset", offset,
                "skippedInfo", info == null ? "Not skipped" : info
        );
    }

    @Operation(summary = "Force clear skipped state for a message (for testing)")
    @PostMapping("/consume/{topic}/partition/{partition}/offset/{offset}/clear-skip")
    public String clearSkipped(@PathVariable String topic,
                               @PathVariable int partition,
                               @PathVariable long offset) {
        // For safety, we expose a method in service to remove skip state (not shown above)
        // Implement service method: removeSkippedMessage(topic, partition, offset)
        // Here we assume it's available
        consumer.clearAllMessages(); // or implement a targeted clear method
        return "✅ Cleared all in-memory state (use targeted clear in production)";
    }
}
