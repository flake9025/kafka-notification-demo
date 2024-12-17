package fr.vvlabs.notification.service.consumer.errors;

import fr.vvlabs.notification.model.ErrorEntity;
import fr.vvlabs.notification.repository.ErrorEntityRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationSilentErrorHandler implements AbstractNotificationErrorHandler<Message<?>> {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.consumer.notification.dlt}")
    private String deadLetterTopic;
    @Value("${spring.kafka.consumer.notification.dlt-smir}")
    private String deadLetterTopicSmir;
    @Autowired(required = false)
    protected Optional<ErrorEntityRepository> errorRecordRepository;
    @Value("${spring.kafka.consumer.notification.dlt-database:false}")
    protected boolean dltDatabaseEnabled;
    private int nbDlt = 0;
    private int nbDltSmir = 0;

    public void sendToDlt(Message<?> message, Exception exception) {
        if(dltDatabaseEnabled) {
            sendToDltDatabase(message, exception);
        }else{
            sendToDltTopic(message, exception);
        }
    }

    public void sendToDltSmir(Message<?> message, Exception exception) {
        if(dltDatabaseEnabled) {
            sendToDltSmirDatabase(message, exception);
        }else{
            sendToDltSmirTopic(message, exception);
        }
    }

    @Override
    public void sendToDltDatabase(Message<?> message, Exception exception) {
        log.error("Generic Error in processing message: {}, sending to database", message.getPayload());
        ErrorEntityRepository repository = errorRecordRepository.orElseThrow(()-> new IllegalStateException("ErrorRecordRepository is null"));
        repository.save(buildErrorEntity(message, exception, "Generic"));
    }

    @Override
    public void sendToDltSmirDatabase(Message<?> message, Exception exception) {
        log.error("SMIR Error in processing message: {}, sending to database", message.getPayload());
        ErrorEntityRepository repository = errorRecordRepository.orElseThrow(()-> new IllegalStateException("ErrorRecordRepository is null"));
        repository.save(buildErrorEntity(message, exception, "SMIR"));
    }

    @Override
    public void sendToDltTopic(Message<?> message, Exception exception) {
        log.error("Generic Error in processing message: {}, sending to {}", message.getPayload(), deadLetterTopic);
        ProducerRecord<String, Object> errorRecord = buildErrorRecord(message, deadLetterTopic);
        kafkaTemplate.send(errorRecord);
        nbDlt++;
        log.error("DLT count: {}", nbDlt);
    }

    @Override
    public void sendToDltSmirTopic(Message<?> message, Exception exception) {
        log.error("SMIR Error in processing message: {}, sending to {}", message.getPayload(), deadLetterTopicSmir);
        ProducerRecord<String, Object> errorRecord = buildErrorRecord(message, deadLetterTopicSmir);
        kafkaTemplate.send(errorRecord);
        nbDltSmir++;
        log.error("DLT SMIR count: {}", nbDltSmir);
    }

    @Override
    public ErrorEntity buildErrorEntity(Message<?> message, Exception exception, String errorType) {
        return new ErrorEntity()
                .setRecord(message.getPayload().toString())
                .setException(exception.getMessage())
                .setErrorType(errorType);
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

