package fr.vvlabs.notification.service.consumer.errors;

import fr.vvlabs.notification.exception.SmirClientTooManyRequestException;
import fr.vvlabs.notification.model.ErrorEntity;
import fr.vvlabs.notification.repository.ErrorEntityRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.messaging.Message;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Optional;

@Slf4j
public class NotificationThrowErrorHandler implements AbstractNotificationErrorHandler<ConsumerRecord<?, ?>> {

    private final DeadLetterPublishingRecoverer genericRecoverer;
    private final DeadLetterPublishingRecoverer smirRecoverer;
    private final String deadLetterTopic;
    private final String deadLetterTopicSmir;
    private final int retries;
    private final long retriesInterval;
    private final boolean retriesSmir;

    protected Optional<ErrorEntityRepository> errorRecordRepository;
    protected boolean dltDatabaseEnabled;

    private int nbDlt = 0;
    private int nbDltSmir = 0;

    public NotificationThrowErrorHandler(
            DeadLetterPublishingRecoverer genericRecoverer,
            DeadLetterPublishingRecoverer smirRecoverer,
            String deadLetterTopic,
            String deadLetterTopicSmir,
            int retries,
            long retriesInterval,
            boolean retriesSmir,
            Optional<ErrorEntityRepository> errorRecordRepository,
            boolean dltDatabaseEnabled) {
        this.genericRecoverer = genericRecoverer;
        this.smirRecoverer = smirRecoverer;
        this.deadLetterTopic = deadLetterTopic;
        this.deadLetterTopicSmir = deadLetterTopicSmir;
        this.retries = retries;
        this.retriesInterval = retriesInterval;
        this.retriesSmir = retriesSmir;
        this.errorRecordRepository = errorRecordRepository;
        this.dltDatabaseEnabled = dltDatabaseEnabled;
    }

    public DefaultErrorHandler createErrorHandler() {
        FixedBackOff fixedBackOff = new FixedBackOff(retriesInterval, retries);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, ex) -> {
                    if (isCauseSmirClientTooManyRequestException(ex)) {
                        if(dltDatabaseEnabled) {
                            sendToDltSmirDatabase(record, ex);
                        } else {
                            sendToDltSmirTopic(record, ex);
                        }
                    } else {
                        if(dltDatabaseEnabled) {
                            sendToDltDatabase(record, ex);
                        } else {
                            sendToDltTopic(record, ex);
                        }
                    }
                },
                fixedBackOff
        );

        if (!retriesSmir) {
            errorHandler.addNotRetryableExceptions(SmirClientTooManyRequestException.class);
        }

        return errorHandler;
    }

    @Override
    public void sendToDltDatabase(ConsumerRecord<?, ?> record, Exception exception) {
        log.error("Generic Error in processing message: {}, sending to database", record.value());
        ErrorEntityRepository repository = errorRecordRepository.orElseThrow(()-> new IllegalStateException("ErrorRecordRepository is null"));
        repository.save(buildErrorEntity(record, exception, "Generic"));
    }

    @Override
    public void sendToDltSmirDatabase(ConsumerRecord<?, ?> record, Exception exception) {
        log.error("SMIR Error in processing message: {}, sending to database", record.value());
        ErrorEntityRepository repository = errorRecordRepository.orElseThrow(()-> new IllegalStateException("ErrorRecordRepository is null"));
        repository.save(buildErrorEntity(record, exception, "SMIR"));
    }

    @Override
    public void sendToDltTopic(ConsumerRecord<?, ?> record, Exception ex) {
        log.error("Generic Error in processing message: {}, sending to {}", record, deadLetterTopic);
        genericRecoverer.accept(record, ex);
        nbDlt++;
        log.error("DLT count: {}", nbDlt);
    }

    @Override
    public void sendToDltSmirTopic(ConsumerRecord<?, ?> record, Exception ex) {
        log.error("SMIR Error in processing message: {}, sending to {}", record, deadLetterTopicSmir);
        smirRecoverer.accept(record, ex);
        nbDltSmir++;
        log.error("DLT SMIR count: {}", nbDltSmir);
    }

    @Override
    public ErrorEntity buildErrorEntity(ConsumerRecord<?, ?> record, Exception exception, String errorType) {
        return new ErrorEntity()
                .setRecord(record.value().toString())
                .setException(exception.getMessage())
                .setErrorType(errorType);
    }

    private boolean isCauseSmirClientTooManyRequestException(Throwable ex) {
        Throwable cause = ex;
        do {
            if (cause instanceof SmirClientTooManyRequestException) {
                return true;
            }
            cause = cause.getCause();
        } while (cause != null);
        return false;
    }
}

