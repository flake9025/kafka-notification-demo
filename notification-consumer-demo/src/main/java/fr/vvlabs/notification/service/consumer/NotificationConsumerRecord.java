package fr.vvlabs.notification.service.consumer;

import fr.vvlabs.notification.config.CommitStrategy;
import fr.vvlabs.notification.config.ConsumerMode;
import fr.vvlabs.notification.exception.SmirClientTooManyRequestException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "spring.kafka.consumer.notification.mode", havingValue = ConsumerMode.RECORD)
@Slf4j
public class NotificationConsumerRecord extends AbstractNotificationConsumer {

    @KafkaListener(
            groupId = "${spring.kafka.consumer.notification.group-id}",
            topics = "${spring.kafka.consumer.notification.topic}",
            containerFactory = "notificationKafkaListenerContainerFactory",
            autoStartup = "#{@kafkaConsumerConfig.isAutoStartup()}"
    )
    public void receiveAuto(Message<String> notificationEns) throws Exception {
        log.info("Notification reçue={}, strategie={}", notificationEns.getPayload(), commitStrategy);

        initStartTime();

        switch (commitStrategy) {
            case CommitStrategy.AUTO, CommitStrategy.MANUAL:
                handleWithCommit(notificationEns, null);
                break;
            case CommitStrategy.TRANSACTION:
                handleWithTransaction(notificationEns);
                break;
            default:
                throw new IllegalStateException("Unsupported commit strategy " + commitStrategy);
        }

        updateTime();
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.notification.group-id}",
            topics = "${spring.kafka.consumer.notification.topic}",
            containerFactory = "notificationKafkaListenerContainerFactory",
            autoStartup = "#{@kafkaConsumerConfig.isManualStartup()}"
    )
    public void receiveManual(Message<String> notificationEns, Acknowledgment acknowledgment) {
        log.info("Notification reçue={}, strategie={}", notificationEns.getPayload(), commitStrategy);

        initStartTime();

        switch (commitStrategy) {
            case CommitStrategy.AUTO, CommitStrategy.MANUAL:
                handleWithCommit(notificationEns, acknowledgment);
                break;
            case CommitStrategy.TRANSACTION:
                handleWithTransaction(notificationEns);
                break;
            default:
                throw new IllegalStateException("Unsupported commit strategy " + commitStrategy);
        }

        updateTime();
    }

    private void handleWithCommit(Message<String> notificationEns, Acknowledgment acknowledgment) {
        log.info("handleWithCommit");
        try {
            processNotification(notificationEns.getPayload());
        } catch (SmirClientTooManyRequestException e) {
            processSmirError(notificationEns, e);
        } catch (Exception e) {
            processGenericError(notificationEns, e);
        }
        commit(acknowledgment);
    }

    private void handleWithTransaction(Message<String> notificationEns) {
        log.info("handleWithTransaction");
        kafkaTemplate.executeInTransaction(operations -> {
            try {
                processNotification(notificationEns.getPayload());
                log.info("Transaction Kafka complétée avec succès");
            } catch (SmirClientTooManyRequestException e) {
                log.warn("Erreur due à l'API SMIR  : {}", e.getMessage());
                processSmirError(notificationEns, e);
            } catch (Exception e) {
                log.warn("Erreur générique  : {}", e.getMessage());
                processGenericError(notificationEns, e);
            }
            // La transaction est commitée à ce stade si aucune exception bloquante n'a été levée
            updateTransactionCount();
            return null;
        });
    }
}
