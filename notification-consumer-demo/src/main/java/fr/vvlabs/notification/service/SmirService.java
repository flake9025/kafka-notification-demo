package fr.vvlabs.notification.service;

import fr.vvlabs.notification.exception.SmirClientTooManyRequestException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
@Slf4j
public class SmirService {

    public String getSmirCoordonnees(String userId) {
        // Générer un délai entre 500 ms et 1 200 ms
        long delay = ThreadLocalRandom.current().nextLong(500, 1201);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Décider aléatoirement de lancer une exception
        boolean shouldThrowException = ThreadLocalRandom.current().nextBoolean();
        if (shouldThrowException) {
            throw new SmirClientTooManyRequestException("Exception lors de l'appel a l'API SMIR apres " + delay + "ms");
        }
        return userId;
    }
}
