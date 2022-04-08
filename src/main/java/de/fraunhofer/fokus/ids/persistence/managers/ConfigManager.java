package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Tuple;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class ConfigManager {
    private Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class.getName());

    private DatabaseConnector databaseConnector;

    public ConfigManager(){
        this.databaseConnector = DatabaseConnector.getInstance();
    }

    public void get(Handler<AsyncResult<JsonObject>> resultHandler){
        databaseConnector.query("SELECT * FROM configuration", Tuple.tuple(),reply -> {
            if(reply.succeeded()){
                if(reply.result().size()>0) {
                    resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
                }
                else{
                    LOGGER.error("No configuration available.");
                    resultHandler.handle(Future.failedFuture("No configuration available."));
                }
            }
            else{
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void edit(Tuple params, Handler<AsyncResult<Void>> resultHandler) {
        databaseConnector.query("UPDATE configuration SET title = $1, maintainer = $2, curator = $3, url = $4, country = $5 WHERE id = $6", params, reply -> {
            if(reply.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });

    }
    public void insert(Tuple params, Handler<AsyncResult<Void>> resultHandler){
        databaseConnector.query("INSERT INTO configuration (title, maintainer, curator, url, country) values ($1,$2,$3,$4,$5)", params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

}
