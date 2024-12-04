package fr.vvlabs.notification.config;

import fr.vvlabs.notification.service.consumer.errors.NotificationThrowErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.notification.group-id}")
    private String groupId;
    @Value("${spring.kafka.consumer.notification.mode:batch}") // record or batch
    private String consumerMode;
    @Value("${spring.kafka.consumer.notification.commit-strategy:auto}") // auto, manual, transaction
    private String commitStrategy;
    @Value("${spring.kafka.consumer.notification.sync-commits:false}")
    private boolean syncCommits;
    @Value("${spring.kafka.consumer.notification.retries:1}")
    private int retries;
    @Value("${spring.kafka.consumer.notification.retries-interval:300000}")
    private int retriesInterval;
    @Value("${spring.kafka.consumer.notification.retries-smir:false}")
    private boolean retriesSmir;
    @Value("${spring.kafka.consumer.notification.dlt}")
    private String deadLetterTopic;
    @Value("${spring.kafka.consumer.notification.dlt-smir}")
    private String deadLetterTopicSmir;
    @Value("${spring.kafka.consumer.notification.max-poll-records:50}")
    private int maxPollRecords;
    @Value("${spring.kafka.consumer.notification.max-poll-interval-ms:300000}")
    private int maxPollInterval;

    @Bean
    public boolean isAutoStartup() {
        return !CommitStrategy.MANUAL.equalsIgnoreCase(commitStrategy);
    }

    @Bean
    public boolean isManualStartup() {
        return CommitStrategy.MANUAL.equalsIgnoreCase(commitStrategy);
    }


    @Bean(name = "genericRecoverer")
    public DeadLetterPublishingRecoverer genericRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            log.warn("Sending to Generic DLT: {}", record);
            return new TopicPartition(deadLetterTopic, record.partition());
        });
    }

    @Bean(name = "smirRecoverer")
    public DeadLetterPublishingRecoverer smirRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            log.warn("Sending to SMIR DLT: {}", record);
            return new TopicPartition(deadLetterTopicSmir, record.partition());
        });
    }

    @Bean
    public NotificationThrowErrorHandler notificationThrowErrorHandler(
            @Qualifier("genericRecoverer") DeadLetterPublishingRecoverer genericRecoverer,
            @Qualifier("smirRecoverer") DeadLetterPublishingRecoverer smirRecoverer) {
        return new NotificationThrowErrorHandler(
                genericRecoverer,
                smirRecoverer,
                deadLetterTopic,
                deadLetterTopicSmir,
                retries,
                retriesInterval,
                retriesSmir
        );
    }

    @Bean
    public DefaultErrorHandler errorHandler(NotificationThrowErrorHandler handler) {
        return handler.createErrorHandler();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> notificationKafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // Configure error handling with retry control / dead letter topics
        factory.setCommonErrorHandler(errorHandler);

        switch (commitStrategy) {
            case CommitStrategy.AUTO :
            case CommitStrategy.TRANSACTION:
                switch (consumerMode) {
                    case ConsumerMode.RECORD:
                        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
                        break;
                    case ConsumerMode.BATCH:
                        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
                        break;
                    default:
                        throw new IllegalStateException("Invalid mode: " + consumerMode);
                }
                break;
            case CommitStrategy.MANUAL :
                factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
                break;
            default:
                throw new IllegalStateException("Invalid commitStrategy: " + commitStrategy);
        }

        // Configuration spécifique au mode batch
        if (ConsumerMode.BATCH.equals(consumerMode)) {
            log.info("mode BATCH activé");
            factory.setBatchListener(true);
            factory.getContainerProperties().setSyncCommits(syncCommits);
        }

        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (ConsumerMode.BATCH.equals(consumerMode)) {
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        }
        switch (commitStrategy) {
            case CommitStrategy.AUTO:
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
                break;
            case CommitStrategy.MANUAL, CommitStrategy.TRANSACTION:
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                break;
            default:
                throw new IllegalStateException("Invalid commitStrategy: " + commitStrategy);
        }
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    @ConditionalOnProperty(name = "spring.kafka.consumer.notification.commit-strategy", havingValue = CommitStrategy.AUTO)
    public ProducerFactory<String, Object> producerFactoryAuto() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    @ConditionalOnProperty(name = "spring.kafka.consumer.notification.commit-strategy", havingValue = CommitStrategy.MANUAL)
    public ProducerFactory<String, Object> producerFactoryManual() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    @ConditionalOnProperty(name = "spring.kafka.consumer.notification.commit-strategy", havingValue = CommitStrategy.TRANSACTION)
    public ProducerFactory<String, Object> producerFactoryTransactional() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-producer-1"); // ID transactionnel unique
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @ConditionalOnProperty(name = "spring.kafka.consumer.notification.commit-strategy", havingValue = CommitStrategy.TRANSACTION)
    public KafkaTransactionManager<String, Object> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(producerFactoryTransactional());
    }
}
