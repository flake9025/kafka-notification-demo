package fr.vvlabs.notification.service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.vvlabs.notification.config.CommitStrategy;
import fr.vvlabs.notification.config.ExceptionStrategy;
import fr.vvlabs.notification.exception.SmirClientTooManyRequestException;
import fr.vvlabs.notification.model.NotificationEvent;
import fr.vvlabs.notification.service.NotificationService;
import fr.vvlabs.notification.service.consumer.errors.NotificationSilentErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;

@Slf4j
public abstract class AbstractNotificationConsumer {

    @Autowired
    protected NotificationService notificationService;
    @Autowired
    protected NotificationSilentErrorHandler notificationErrorHandler;
    @Autowired
    protected ObjectMapper objectMapper;
    @Autowired
    protected KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.consumer.notification.commit-strategy:auto}") // auto, manual, transaction
    protected String commitStrategy;
    @Value("${spring.kafka.consumer.notification.exception-strategy:throw}") // silent, throw
    protected String exceptionStrategy;
    @Value("${spring.kafka.consumer.notification.exception-strategy-smir:silent}") //  silent, throw
    protected String exceptionStrategySmir;

    protected long startTime = 0;
    protected int totalMessageCount = 0;
    protected int totalCommits = 0;
    protected int totalDlt = 0;
    protected int totalDltSmir = 0;
    protected int totalTransactions = 0;
    protected long totalProcessingTime = 0;
    protected void processNotification(String notificationEns) throws Exception {
        NotificationEvent notificationEvent = objectMapper.readValue(notificationEns, NotificationEvent.class);
        notificationService.buildAndSendNotification(notificationEvent);
    }
    protected void commit(Acknowledgment acknowledgment) {
        log.info("Commit strategy {} ...", commitStrategy);
        if (CommitStrategy.MANUAL.equals(commitStrategy)) {
            log.info("\tcommit !");
            if(acknowledgment != null) {
                acknowledgment.acknowledge();
            } else {
                log.error("\tacknowledgment is null.");
            }
            totalCommits++;
        } else {
            log.info("\tskipping commit.");
        }
    }

    protected void updateTransactionCount() {
        totalTransactions++;
    }
    protected void processSmirError(Message<String> message, SmirClientTooManyRequestException e) {
        log.warn("Erreur due à l'API SMIR  : {}", e.getMessage());
        totalDltSmir++;
        if(ExceptionStrategy.THROW.equals(exceptionStrategySmir)) {
            throw new RuntimeException(e);
        } else {
            notificationErrorHandler.sendToDltSmir(message, e);
        }
    }

    protected void processGenericError(Message<String> message, Exception e) {
        log.warn("Erreur generique  : {}", e.getMessage());
        totalDlt++;
        if(ExceptionStrategy.THROW.equals(exceptionStrategy)) {
            throw new RuntimeException(e);
        } else {
            notificationErrorHandler.sendToDlt(message, e);
        }
    }

    protected void initStartTime() {
        if (totalMessageCount == 0) {
            startTime = System.currentTimeMillis();
        }
    }

    protected void updateTime() {
        updateTime(1);
    }
    protected void updateTime(int nbMessages) {
        totalMessageCount += nbMessages;
        // Calculer et accumuler le temps de traitement total après chaque message
        totalProcessingTime = System.currentTimeMillis() - startTime;
        // Loguer le temps total de traitement pour le lot de messages
        log.info("Total time for {} notifications: {} ms", totalMessageCount, totalProcessingTime);
        if (CommitStrategy.MANUAL.equals(commitStrategy)) {
            log.info("Total commits : {}", totalCommits);
        }
        if (CommitStrategy.TRANSACTION.equals(commitStrategy)) {
            log.info("Total transactions : {} ", totalTransactions);
        }
        log.info("Total DLT :{} ", totalDlt);
        log.info("Total DLT SMIR : {} ", totalDltSmir);
    }
}

