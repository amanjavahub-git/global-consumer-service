package com.pharmacy.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * GlobalConsumerService with retry-limit (2) and skip-on-exceed behavior.
 * - Tracks per-message retries using a composite key: topic|partition|offset
 * - If retries >= 2 -> commit offset and move on (message skipped)
 * - Successful processing -> acknowledge() (manual commit)
 */
@Service
public class GlobalConsumerService {

    // Store messages per topic for retrieval
    private final Map<String, Queue<String>> topicMessages = new ConcurrentHashMap<>();

    // Track offset info for debugging
    private final Map<String, String> offsetInfo = new ConcurrentHashMap<>();

    // Track retry counts per message (key = topic|partition|offset)
    private final Map<String, Integer> retryCounts = new ConcurrentHashMap<>();

    // Track skipped messages for auditing (key -> reason/timestamp)
    private final Map<String, String> skippedMessages = new ConcurrentHashMap<>();

    // Max retries before skipping
    private static final int MAX_RETRIES = 2;

    @KafkaListener(
            topics = "${app.kafka.consumer-topics}",
            groupId = "${spring.kafka.consumer.group-id:global-consumer}",
            concurrency = "${spring.kafka.listener.concurrency:3}"
    )
    public void consume(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        String msgId = buildMessageId(topic, partition, offset);

        try {
            // If this message was previously skipped, just acknowledge and return
            if (isSkipped(msgId)) {
                if (acknowledgment != null) {
                    acknowledgment.acknowledge(); // ensure offset committed
                }
                return;
            }

            // Process message (replace with real business logic)
            boolean processed = processMessageSafely(message, topic, partition, offset, key);

            if (processed) {
                // On success: store and commit offset
                storeMessage(topic, formatMessage(topic, partition, offset, key, message));
                offsetInfo.put(topic + "-p" + partition, "Offset: " + offset);
                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
                // clear retry count on success
                retryCounts.remove(msgId);
            } else {
                // Processing failed -> increment retry count
                int retries = retryCounts.getOrDefault(msgId, 0) + 1;
                retryCounts.put(msgId, retries);

                if (retries >= MAX_RETRIES) {
                    // Exceeded retries -> skip permanently: commit offset and log
                    skippedMessages.put(msgId, "Skipped after " + retries + " retries at " + Instant.now());
                    if (acknowledgment != null) {
                        acknowledgment.acknowledge(); // commit so it won't be reprocessed
                    }
                    System.out.println(" Skipped message " + msgId + " after " + retries + " retries.");
                } else {
                    // Not yet exceeded -> do NOT acknowledge so Kafka may redeliver
                    System.out.println(" Processing failed for " + msgId + ". Retry " + retries + "/" + MAX_RETRIES);
                }
            }
        } catch (Exception e) {
            // Unexpected exception: do not acknowledge so message can be retried
            System.err.println(" Unexpected error for " + msgId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Helper: build unique id for message
    private String buildMessageId(String topic, int partition, long offset) {
        return topic + "|" + partition + "|" + offset;
    }

    // Helper: check if message was skipped earlier
    private boolean isSkipped(String msgId) {
        return skippedMessages.containsKey(msgId);
    }

    // Replace this with actual business logic; return true if processed successfully
    private boolean processMessageSafely(String message, String topic, int partition, long offset, String key) {
        try {
            // Example: simple validation; real logic may parse JSON, call DB, etc.
            if (message == null || message.trim().isEmpty()) {
                return false;
            }
            // Simulate processing success for most messages
            // If you want deterministic failure for testing, add conditions here
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private String formatMessage(String topic, int partition, long offset, String key, String value) {
        return String.format("Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s",
                topic, partition, offset, key, value);
    }

    private void storeMessage(String topic, String formattedMsg) {
        topicMessages.putIfAbsent(topic, new ConcurrentLinkedQueue<>());
        topicMessages.get(topic).add(formattedMsg);
        System.out.println(" Consumed and stored: " + formattedMsg);
    }

    // Public APIs used by controller

    public Queue<String> getMessagesByTopic(String topic) {
        return topicMessages.getOrDefault(topic, new ConcurrentLinkedQueue<>());
    }

    public String getLastMessageByTopic(String topic) {
        Queue<String> msgs = topicMessages.get(topic);
        return (msgs == null || msgs.isEmpty()) ? "No messages consumed yet for topic: " + topic : msgs.peek();
    }

    public Queue<String> getAndClearMessagesByTopic(String topic) {
        Queue<String> msgs = topicMessages.getOrDefault(topic, new ConcurrentLinkedQueue<>());
        topicMessages.put(topic, new ConcurrentLinkedQueue<>());
        return msgs;
    }

    public String getOffsetInfo(String topic, int partition) {
        return offsetInfo.getOrDefault(topic + "-p" + partition, "No offset info available");
    }

    public Map<String, String> getAllOffsetInfo() {
        return new ConcurrentHashMap<>(offsetInfo);
    }

    public void clearAllMessages() {
        topicMessages.clear();
        offsetInfo.clear();
        retryCounts.clear();
        skippedMessages.clear();
        System.out.println(" All in-memory messages and retry/skipped state cleared");
    }

    public int getMessageCountByTopic(String topic) {
        return topicMessages.getOrDefault(topic, new ConcurrentLinkedQueue<>()).size();
    }

    // Expose retry and skipped info for debugging
    public Integer getRetryCount(String topic, int partition, long offset) {
        return retryCounts.get(buildMessageId(topic, partition, offset));
    }

    public String getSkippedInfo(String topic, int partition, long offset) {
        return skippedMessages.get(buildMessageId(topic, partition, offset));
    }
}
