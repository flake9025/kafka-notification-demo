package fr.vvlabs.notification.service;

import fr.vvlabs.notification.record.NotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final SmirService smirService;

    public void buildAndSendNotification(NotificationEvent notificationEvent) {
        log.info("Début de traitement Eip ENS notification : {}", notificationEvent);

        switch (notificationEvent.getEvent()){
            case "AJOUT_DOCUMENT" :
                log.info("Ajout de document ... OK");

                // Introduire une exception IOException aléatoire 2% du temps
                if (Math.random() < 0.02) { // 1% de chances
                    log.error("Erreur IOException simulée pendant l'ajout de document.");
                    throw new RuntimeException("Erreur simulée lors de l'ajout du document.");
                }
                break;
            case "OUVERTURE_ENS" :
                log.info("Ouverture ENS ... appel API SMIR");
                smirService.getSmirCoordonnees(notificationEvent.getUserId());
                log.info("Ouverture ENS ... OK");
                break;
            case "INCITATION_ENROLEMENT" :
                log.info("Incitation Enrolement : appel API SMIR");
                smirService.getSmirCoordonnees(notificationEvent.getUserId());
                log.info("Incitation Enrolement ... OK");
                break;
        }
        log.info("Fin de traitement Eip ENS notification : {}", notificationEvent);
    }
}
