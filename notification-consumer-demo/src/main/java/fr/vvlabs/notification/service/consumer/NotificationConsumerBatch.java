package fr.vvlabs.notification.service.consumer;

import fr.vvlabs.notification.config.CommitStrategy;
import fr.vvlabs.notification.config.ConsumerMode;
import fr.vvlabs.notification.exception.SmirClientTooManyRequestException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "spring.kafka.consumer.notification.mode", havingValue = ConsumerMode.BATCH)
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumerBatch extends AbstractNotificationConsumer {

    @KafkaListener(
            groupId = "${spring.kafka.consumer.notification.group-id}",
            topics = "${spring.kafka.consumer.notification.topic}",
            containerFactory = "notificationKafkaListenerContainerFactory",
            autoStartup = "#{@kafkaConsumerConfig.isAutoStartup()}"
    )
    public void receiveBatchAuto(List<Message<String>> notifications) throws Exception {
        log.info("Lot de notifications reçu, nombre de messages : {}, stratégie={}", notifications.size(), commitStrategy);

        initStartTime();

        switch (commitStrategy) {
            case CommitStrategy.AUTO, CommitStrategy.MANUAL:
                handleWithCommit(notifications, null);
                break;
            case CommitStrategy.TRANSACTION:
                handleWithTransaction(notifications);
                break;
            default:
                throw new IllegalStateException("Unsupported commit strategy " + commitStrategy);
        }

        updateTime(notifications.size());
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.notification.group-id}",
            topics = "${spring.kafka.consumer.notification.topic}",
            containerFactory = "notificationKafkaListenerContainerFactory",
            autoStartup = "#{@kafkaConsumerConfig.isManualStartup()}"
    )
    public void receiveBatchManual(List<Message<String>> notifications, Acknowledgment acknowledgment) {
        log.info("Lot de notifications reçu, nombre de messages : {}, stratégie={}", notifications.size(), commitStrategy);

        initStartTime();

        switch (commitStrategy) {
            case CommitStrategy.AUTO, CommitStrategy.MANUAL:
                handleWithCommit(notifications, acknowledgment);
                break;
            case CommitStrategy.TRANSACTION:
                handleWithTransaction(notifications);
                break;
            default:
                throw new IllegalStateException("Unsupported commit strategy " + commitStrategy);
        }

        updateTime(notifications.size());
    }

    private void handleWithCommit(List<Message<String>> notifications, Acknowledgment acknowledgment) {
        log.info("handleWithCommit");
        for (Message<String> message : notifications) {
            try {
                String notificationEns = message.getPayload();
                processNotification(notificationEns);
            } catch (SmirClientTooManyRequestException e) {
                processSmirError(message, e);
            } catch (Exception e) {
                processGenericError(message, e);
            }
        }
        commit(acknowledgment);
    }

    private void handleWithTransaction(List<Message<String>> notifications) {
        log.info("handleWithTransaction");

        // Traiter l'ensemble du lot dans une transaction Kafka
        kafkaTemplate.executeInTransaction(operations -> {
            for (Message<String> message : notifications) {
                try {
                    String notificationEns = message.getPayload();
                    processNotification(notificationEns);
                } catch (SmirClientTooManyRequestException e) {
                    processSmirError(message, e);
                } catch (Exception e) {
                    processGenericError(message, e);
                }
            }
            log.info("Transaction Kafka complétée avec succès pour le lot");
            // La transaction est commitée à ce stade si aucune exception bloquante n'a été levée
            updateTransactionCount();
            return null;
        });
    }
}