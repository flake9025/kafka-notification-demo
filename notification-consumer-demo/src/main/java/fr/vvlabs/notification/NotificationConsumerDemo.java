package fr.vvlabs.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

@SpringBootApplication
public class NotificationConsumerDemo {

    public static void main(String[] args) {
        // Démarrage de l'application et récupération du contexte Spring
        ConfigurableApplicationContext context = SpringApplication.run(NotificationConsumerDemo.class, args);

        // Récupération de l'environnement pour accéder aux propriétés
        Environment env = context.getEnvironment();

        // Récupération de la propriété server.url
        String serverUrl = env.getProperty("server.url");

        // Construction de l'URL Swagger et affichage
        System.out.println("----------------------------------------------------------------");
        System.out.println("Swagger UI available at: " + serverUrl + "/swagger-ui/index.html");
        System.out.println("H2 available at: " + serverUrl + "/h2-console");
        System.out.println("Actuator available at: " + serverUrl + "/actuator");
    }
}
