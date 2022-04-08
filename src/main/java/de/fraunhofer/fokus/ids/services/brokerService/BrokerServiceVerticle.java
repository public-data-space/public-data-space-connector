package de.fraunhofer.fokus.ids.services.brokerService;

import de.fraunhofer.fokus.ids.models.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class BrokerServiceVerticle extends AbstractVerticle {

    private Logger LOGGER = LoggerFactory.getLogger(BrokerServiceVerticle.class.getName());

    @Override
    public void start(Promise<Void> startPromise) {

        WebClient webClient = WebClient.create(vertx);
        BrokerService.create(vertx, webClient, ready -> {
            if (ready.succeeded()) {
                ServiceBinder binder = new ServiceBinder(vertx);
                binder
                        .setAddress(Constants.BROKER_SERVICE)
                        .register(BrokerService.class, ready.result());
                LOGGER.info("BrokerService successfully started.");
                startPromise.complete();
            } else {
                LOGGER.error(ready.cause());
                startPromise.fail(ready.cause());
            }
        });
    }
}