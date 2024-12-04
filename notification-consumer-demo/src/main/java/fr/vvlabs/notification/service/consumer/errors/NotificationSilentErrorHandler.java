package fr.vvlabs.notification.service.consumer.errors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationSilentErrorHandler {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.consumer.notification.dlt}")
    private String deadLetterTopic;
    @Value("${spring.kafka.consumer.notification.dlt-smir}")
    private String deadLetterTopicSmir;

    private int nbDlt = 0;
    private int nbDltSmir = 0;
    public Object sendToDlt(Message<?> message, Exception exception) {
        log.error("Generic Error in processing message: {}, sending to {}", message.getPayload(), deadLetterTopic);
        ProducerRecord<String, Object> errorRecord = buildErrorRecord(message, deadLetterTopic);
        kafkaTemplate.send(errorRecord);
        nbDlt++;
        log.error("DLT count: {}", nbDlt);
        return message.getPayload();
    }

    public Object sendToDltSmir(Message<?> message, Exception exception) {
        log.error("SMIR Error in processing message: {}, sending to {}", message.getPayload(), deadLetterTopicSmir);
        ProducerRecord<String, Object> errorRecord = buildErrorRecord(message, deadLetterTopicSmir);
        kafkaTemplate.send(errorRecord);
        nbDltSmir++;
        log.error("DLT SMIR count: {}", nbDltSmir);
        return message.getPayload();
    }

    private ProducerRecord<String, Object> buildErrorRecord(Message<?> message, String destinationTopic) {
        String key = message.getHeaders().get(KafkaHeaders.RECEIVED_KEY, String.class);
        Integer partition = message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION, Integer.class);
        Object payload = message.getPayload();
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
                destinationTopic, partition, key, payload);
        // Recopier les en-têtes
        message.getHeaders().forEach((headerKey, headerValue) -> {
            if (headerValue instanceof String || headerValue instanceof byte[]) {
                producerRecord.headers().add(headerKey, headerValue.toString().getBytes());
            }
        });
        return producerRecord;
    }
}

