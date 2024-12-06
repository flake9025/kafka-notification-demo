package fr.vvlabs.notification.record;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NotificationEvent {

  private String event;
  private String userId;
  private String ipAddress;
  private String userAgent;
  private Integer messageNumber;
}
