package com.dailycodebuffer.cabbookuser.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Info: The below code is for a normal project flow
 */
//@Service
//public class LocationService {
//
//    @KafkaListener(topics = "cab-location", groupId = "user-group")
//    public void cabLocation(String location) {
//        System.out.println(location);
//    }
//}

/**
 * Info: The below code is for error + Retry + Dead Letter Topic queue
 */
//@Service
//public class LocationService {
//
//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;
//
//    @RetryableTopic(
//            attempts = "1",
//            backoff = @Backoff(delay = 500), // 2 seconds delay between retries
//            dltTopicSuffix = ".dlq"
//    )
//    @KafkaListener(topics = "cab-location", groupId = "user-group")
//    public void cabLocation(String location) {
//        System.out.println("Received: " + location);
//        // Simulating failure
//        throw new RuntimeException("Simulated failure");
//    }
//
//    @KafkaListener(topics = "cab-location.dlq", groupId = "user-group")
//    public void dltListener(String failedMessage) {
//        System.out.println("Received in DLT: " + failedMessage);
//        // Optionally, send to a manual DLQ or alert system
//        kafkaTemplate.send("manual-dead-letter-topic", failedMessage);
//    }
//}


/**
 * Info: The below code is for error + Retry + Dead Letter Topic queue  for only first message
 */

@Service
public class LocationService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // This flag ensures the exception is thrown only once
    private final AtomicBoolean hasFailedOnce = new AtomicBoolean(false);

    @RetryableTopic(
            attempts = "1",
            backoff = @Backoff(delay = 500),
            dltTopicSuffix = ".dlq"
    )
    @KafkaListener(topics = "cab-location", groupId = "user-group")
    public void cabLocation(String location) {
        System.out.println("Received: " + location);

        // Simulate failure only for the first message
        if (!hasFailedOnce.getAndSet(true)) {
            throw new RuntimeException("Simulated failure");
        }

        // Continue normally for other messages
        System.out.println("Processed successfully: " + location);
    }

    @KafkaListener(topics = "cab-location.dlq", groupId = "user-group")
    public void dltListener(String failedMessage) {
        System.out.println("Received in DLT: " + failedMessage);
        kafkaTemplate.send("manual-dead-letter-topic", failedMessage);
    }
}

