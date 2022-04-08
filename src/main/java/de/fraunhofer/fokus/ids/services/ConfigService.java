package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.managers.ConfigManager;
import de.fraunhofer.fokus.ids.services.brokerService.BrokerService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Tuple;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class ConfigService {

    private Logger LOGGER = LoggerFactory.getLogger(ConfigService.class.getName());
    private ConfigManager configManager;
    private BrokerService brokerService;

    public ConfigService(Vertx vertx){
        this.configManager  = new ConfigManager();
        this.brokerService = BrokerService.createProxy(vertx, Constants.BROKER_SERVICE);
    }

    private void getConfig(Handler<AsyncResult<JsonObject>> resultHandler){
        configManager.get(reply -> resultHandler.handle(reply));
    }

    public void editConfiguration(JsonObject json, Handler<AsyncResult<JsonObject>> resultHandler) {

        Tuple params = Tuple.tuple()
                .addString(json.getString("title"))
                .addString(json.getString("maintainer"))
                .addString(json.getString("curator"))
                .addString(json.getString("url"))
                .addString(json.getString("country"));


        configManager.get(reply -> {
            if(reply.succeeded()){
                params.addLong(reply.result().getLong("id"));
                    edit(params, resultHandler);
            } else {
                insert(params, resultHandler);
            }
        });
    }


    public void edit(Tuple params, Handler<AsyncResult<JsonObject>> resultHandler){

        brokerService.unsubscribeAll(unsubReply -> {
            if (unsubReply.succeeded()) {
                configManager.edit(params, editReply -> {
                    if (editReply.succeeded()) {
                        brokerService.subscribeAll(subReply -> {
                            if (subReply.succeeded()) {
                                JsonObject jO = new JsonObject();
                                jO.put("status", "success");
                                jO.put("text", "Konfiguration geändert");
                                resultHandler.handle(Future.succeededFuture(jO));
                            } else {
                                JsonObject jO = new JsonObject();
                                jO.put("status", "info");
                                jO.put("text", "Konfiguration wurde geändert, aber Fehler bei der Brokeranmeldung.");
                                resultHandler.handle(Future.succeededFuture(jO));
                            }
                        });
                    } else {
                        LOGGER.error(editReply.cause());
                        resultHandler.handle(Future.failedFuture(editReply.cause()));
                    }
                });
            } else {
                LOGGER.error(unsubReply.cause());
                JsonObject jO = new JsonObject();
                jO.put("status", "error");
                jO.put("text", "Konfiguration konnte nicht geändert werden.");
                resultHandler.handle(Future.succeededFuture(jO));
            }
        });
    }

    public void insert(Tuple params, Handler<AsyncResult<JsonObject>> resultHandler){
        configManager.insert(params, r -> reply(r, resultHandler));
    }

    private void reply(AsyncResult reply, Handler<AsyncResult<JsonObject>> resultHandler){
        if (reply.succeeded()) {
            JsonObject jO = new JsonObject();
            jO.put("status", "success");
            jO.put("text", "Konfiguration geändert");
            resultHandler.handle(Future.succeededFuture(jO));
        } else {
            JsonObject jO = new JsonObject();
            jO.put("status", "error");
            jO.put("text", "Konfiguration konnte nicht geändert werden.");
            resultHandler.handle(Future.succeededFuture(jO));
        }
    }

    public void getConfiguration(Handler<AsyncResult<JsonObject>> resultHandler){
        getConfig( reply -> {
            if(reply.succeeded()){
                resultHandler.handle(Future.succeededFuture(reply.result()));
            } else {
                JsonObject jO = new JsonObject();
                jO.put("status", "info");
                jO.put("text", "Noch keine Konfiguration vorhanden.");
                resultHandler.handle(Future.succeededFuture(jO));
            }
        });
    }
}
