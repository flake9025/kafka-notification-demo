package fr.vvlabs.notification.repository;

import fr.vvlabs.notification.model.ErrorEntity;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;

@ConditionalOnProperty(name = "spring.kafka.consumer.notification.dlt-database", havingValue = "true")
public interface ErrorEntityRepository extends JpaRepository<ErrorEntity, Long> {

    void deleteAllByCreatedAtBefore(LocalDateTime cutoff);
}

