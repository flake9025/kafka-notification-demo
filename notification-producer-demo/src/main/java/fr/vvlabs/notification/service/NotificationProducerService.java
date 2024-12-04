package fr.vvlabs.notification.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import fr.vvlabs.notification.model.NotificationEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();

    @Value("${spring.kafka.producer.topic-name}")
    private String topicName;
    @Value("${spring.kafka.producer.delay-ms:1000}")
    private int delay;
    @Value("${spring.kafka.producer.max-messages:1000}")
    private int maxMessages;

    @PostConstruct
    public void process() throws Exception {
        int messageCount = 0;
        do {
            String userId = UUID.randomUUID().toString();
            messageCount++;
            String eventType = generateRandomEvent();
            NotificationEvent authentification = NotificationEvent
                    .builder()
                    .event(eventType)
                    .userId(userId)
                    .messageNumber(messageCount)
                    .ipAddress("127.0.0.1")
                    .userAgent("chrome")
                    .build();

            String authJson = objectMapper.writeValueAsString(authentification);
            log.info("sending notification='{}'", authJson);
            kafkaTemplate.send(topicName, authJson);
            Thread.sleep(delay);
        } while(messageCount < maxMessages);
    }

    private String generateRandomEvent() {
        double randomValue = random.nextDouble() * 100;
        if (randomValue < 90) {
            return "AJOUT_DOCUMENT";
        } else if (randomValue < 95) {
            return "OUVERTURE_ENS";
        } else {
            return "INCITATION_ENROLEMENT";
        }
    }
}
