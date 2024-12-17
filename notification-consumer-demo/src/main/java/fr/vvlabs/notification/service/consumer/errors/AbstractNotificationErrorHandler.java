package fr.vvlabs.notification.service.consumer.errors;

import fr.vvlabs.notification.model.ErrorEntity;

public interface AbstractNotificationErrorHandler<T> {

    void sendToDltDatabase(T record, Exception exception);

    void sendToDltSmirDatabase(T record, Exception exception);

    void sendToDltTopic(T record, Exception exception);

    void sendToDltSmirTopic(T record, Exception exception);

    ErrorEntity buildErrorEntity(T record, Exception exception, String errorType);
}
