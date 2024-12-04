package fr.vvlabs.notification.exception;

public class SmirClientTooManyRequestException extends RuntimeException {

    public SmirClientTooManyRequestException(String message) {
        super(message);
    }
}
