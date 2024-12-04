package fr.vvlabs.notification.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class NotificationEvent {

    private String event;
    private String userId;
    private String ipAddress;
    private String userAgent;
    private Integer messageNumber;
}
