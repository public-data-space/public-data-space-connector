package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.managers.BrokerManager;
import de.fraunhofer.fokus.ids.persistence.util.BrokerStatus;
import de.fraunhofer.fokus.ids.services.brokerService.BrokerService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.MalformedURLException;
import java.net.UnknownHostException;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class BrokerController {
    private Logger LOGGER = LoggerFactory.getLogger(BrokerController.class.getName());

    private BrokerManager brokerManager;
    private BrokerService brokerService;

    public BrokerController(Vertx vertx){
        this.brokerManager = new BrokerManager();
        this.brokerService = BrokerService.createProxy(vertx, Constants.BROKER_SERVICE);
    }

    public void register(long id, Handler<AsyncResult<JsonObject>> resultHandler){

        brokerManager.findById(id, findIdReply -> {
            if(findIdReply.succeeded()){
                brokerService.subscribe(findIdReply.result().getString("url"), subscribeReply -> {
                    if(subscribeReply.succeeded()) {
                        LOGGER.info("Registration at broker successful.");
                        brokerManager.register(id, registrationReply -> {
                            if (registrationReply.succeeded()) {
                                JsonObject jO = new JsonObject()
                                        .put("info", new JsonObject()
                                                .put("status", "success")
                                                .put("text", "Brokerregistrierung erfolgreich."))
                                        .put("data", registrationReply.result());
                                resultHandler.handle(Future.succeededFuture(jO));
                            } else {
                                //If this fails, the connector is registered to the broker, but  the broker is not marked registered in the database
                                //therefore try to clean the state. If this fails, log the inconsistent state
                                LOGGER.error(registrationReply.cause());
                                brokerService.unsubscribe(findIdReply.result().getString("url"), reply -> {
                                    if(reply.failed()){
                                        LOGGER.error("INCONSISTENT STATE: Broker could not be unregistered at remote but was unregistered in database.");
                                    }
                                });
                                resultHandler.handle(Future.succeededFuture(registrationError()));
                            }
                        });
                    }else{
                        LOGGER.error(subscribeReply.cause());
                        resultHandler.handle(Future.succeededFuture(registrationError()));
                    }
                });
            }
            else{
                LOGGER.error(findIdReply.cause());
                resultHandler.handle(Future.succeededFuture(registrationError()));
            }
        });
    }

    private JsonObject registrationError(){
        JsonObject jO = new JsonObject();
        jO.put("status", "error");
        jO.put("text", "Broker konnte nicht registriert werden.");
        return jO;
    }

    private JsonObject unregistrationError(){
        JsonObject jO = new JsonObject();
        jO.put("status", "error");
        jO.put("text", "Broker konnte nicht abgemeldet werden.");
        return jO;
    }

    public void unregister(long id, Handler<AsyncResult<JsonObject>> resultHandler){

        brokerManager.findById(id, findIdReply -> {
            if(findIdReply.succeeded()){
                brokerService.unsubscribe(findIdReply.result().getString("url"), unsubscribeReply -> {
                    if(unsubscribeReply.succeeded()) {
                        brokerManager.unregister(id, unregisterReply -> {
                            if (unregisterReply.succeeded()) {
                                LOGGER.info("Unregistration at broker successful.");
                                JsonObject jO = new JsonObject();
                                jO.put("status", "success");
                                jO.put("text", "Brokerabmeldung erfolgreich.");
                                resultHandler.handle(Future.succeededFuture(jO));
                            } else {
                                //If this fails, the connector is not registered to the broker, but the broker is marked registered in the database
                                //therefore try to clean the state. If this fails, log the inconsistent state
                                brokerService.subscribe(findIdReply.result().getString("url"), reply -> {
                                    if(reply.failed()){
                                        LOGGER.error("INCONSISTENT STATE: Broker could not be registered at remote but was registered in database.");
                                    }
                                });
                                LOGGER.error(unsubscribeReply.cause());
                                resultHandler.handle(Future.succeededFuture(unregistrationError()));
                            }
                        });
                    }
                    else{
                        LOGGER.error(unsubscribeReply.cause());
                        resultHandler.handle(Future.succeededFuture(unregistrationError()));
                    }
                });
            }
            else{
                LOGGER.error(findIdReply.cause());
                resultHandler.handle(Future.succeededFuture(unregistrationError()));
            }
        });
    }

    public void add(String url, Handler<AsyncResult<JsonObject>> resultHandler){
        brokerManager.add(url, addReply -> {
            if(addReply.succeeded()) {
                brokerService.subscribe(url, subscribeReply -> {
                    if (subscribeReply.succeeded()) {
                        LOGGER.info("Adding of new broker successful.");
                        JsonObject jO = new JsonObject();
                        jO.put("status", "success");
                        jO.put("text", "Brokeranmeldung erfolgreich.");
                        resultHandler.handle(Future.succeededFuture(jO));
                    } else {
                        //If this fails, the connector is not registered to the broker, but the broker is marked still in the database
                        //therefore try to clean the state. If this fails, log the inconsistent state
                        LOGGER.error(subscribeReply.cause());
                        brokerManager.unregisterByUrl(url, unregisterReply -> {
                            if (unregisterReply.failed()) {
                                LOGGER.error("INCONSISTENT STATE: Broker could not be registered at remote but is registered in database.");
                            }
                        } );
                        resultHandler.handle(Future.succeededFuture(registrationError()));
                    }
                });
            }
            else{
                LOGGER.error(addReply.cause());
                resultHandler.handle(Future.succeededFuture(registrationError()));
            }
        });
    }

    public void update(Handler<AsyncResult> resultHandler){
        brokerService.update( reply -> {
            if (reply.succeeded()) {
                LOGGER.info("Updating of new connector information at brokers successful.");
                resultHandler.handle(Future.succeededFuture());
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void delete(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        brokerManager.findById(id, findIdReply -> {
            if(findIdReply.succeeded()) {
                if (findIdReply.result().getString("status").equals(BrokerStatus.REGISTERED.name())) {
                    brokerService.unsubscribe(findIdReply.result().getString("url"), unsubscribeReply -> {
                        if (unsubscribeReply.succeeded()
                                || unsubscribeReply.cause().getClass().equals(MalformedURLException.class)
                                || unsubscribeReply.cause().getClass().equals(UnknownHostException.class)) {
                            brokerManager.delete(id, deleteReply -> {
                                if (deleteReply.succeeded()) {
                                    LOGGER.info("Broker successfully deleted.");
                                    JsonObject jO = new JsonObject();
                                    jO.put("status", "success");
                                    jO.put("text", "Löschen des Brokers erfolgreich.");
                                    resultHandler.handle(Future.succeededFuture(jO));
                                } else {
                                    //If this fails, the connector is not registered to the broker, but the broker is marked still in the database
                                    //therefore try to clean the state. If this fails, log the inconsistent state
                                    brokerService.subscribe(findIdReply.result().getString("url"), reply -> {
                                        if (reply.failed()) {
                                            LOGGER.error("INCONSISTENT STATE: Broker could not be registered at remote but is registered in database.");
                                        }
                                    });
                                }
                            });
                        } else {
                            LOGGER.error(unsubscribeReply.cause());
                            resultHandler.handle(Future.succeededFuture(unregistrationError()));
                        }
                    });
                } else {
                    brokerManager.delete(id, deleteReply -> {
                        if (deleteReply.succeeded()) {
                            LOGGER.info("Broker successfully deleted.");
                            JsonObject jO = new JsonObject();
                            jO.put("status", "success");
                            jO.put("text", "Löschen des Brokers erfolgreich.");
                            resultHandler.handle(Future.succeededFuture(jO));
                        } else {
                            LOGGER.error(deleteReply.cause());
                            resultHandler.handle(Future.succeededFuture(unregistrationError()));
                        }
                    });
                }
            } else{
                LOGGER.error(findIdReply.cause());
                resultHandler.handle(Future.succeededFuture(unregistrationError()));
            }
        });
    }
}
