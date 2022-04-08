package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.persistence.util.BrokerStatus;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Tuple;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class BrokerManager {

    private Logger LOGGER = LoggerFactory.getLogger(DataAssetManager.class.getName());
    private DatabaseConnector databaseConnector;

    private static final String UPDATE_QUERY = "INSERT INTO Broker (created_at, updated_at, url, status) values (NOW(), NOW(), $1, $2)";
    private static final String UNREGISTER_QUERY =  "Update Broker SET updated_at = NOW(), status = $1  WHERE id = $2";
    private static final String UNREGISTERBYURL_QUERY =  "Update Broker SET updated_at = NOW(), status = $1  WHERE url = $2";
    private static final String REGISTER_QUERY =  "Update Broker SET updated_at = NOW(), status = $1  WHERE id = $2";
    private static final String FINDALL_QUERY = "SELECT * FROM Broker";
    private static final String FINDBYSTATUS_QUERY = "SELECT * FROM Broker WHERE status = $1";
    private static final String FINDBYID_QUERY = "SELECT * FROM Broker WHERE id = $1";
    private static final String FINDBYCREATE_QUERY = "SELECT * FROM Broker WHERE created_at = $1";
    private static final String DELETE_QUERY = "DELETE FROM Broker WHERE id = $1";

    public BrokerManager() {
        databaseConnector = DatabaseConnector.getInstance();
    }

    public void add(String url, Handler<AsyncResult<Void>> resultHandler){
        Tuple params = Tuple.tuple()
                .addString(url)
                .addString(BrokerStatus.REGISTERED.toString());

        databaseConnector.query(UPDATE_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void unregister(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        Tuple params = Tuple.tuple()
                .addString(BrokerStatus.UNREGISTERED.toString())
                .addLong(id);

        performUnregister(UNREGISTER_QUERY, params, resultHandler);

    }

    public void delete(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        Tuple params = Tuple.tuple()
                .addLong(id);

        databaseConnector.query(DELETE_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void unregisterByUrl(String url, Handler<AsyncResult<JsonObject>> resultHandler){
        Tuple params = Tuple.tuple()
                .addString(BrokerStatus.UNREGISTERED.toString())
                .addString(url);

        performUnregister(UNREGISTERBYURL_QUERY, params, resultHandler);
    }

    private void performUnregister(String query, Tuple params, Handler<AsyncResult<JsonObject>> resultHandler ){
        databaseConnector.query(query, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void register(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        Tuple params = Tuple.tuple()
                .addString(BrokerStatus.REGISTERED.toString())
                .addLong(id);

        databaseConnector.query(REGISTER_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void findAll(Handler<AsyncResult<JsonArray>> resultHandler){
        databaseConnector.query(FINDALL_QUERY, Tuple.tuple(), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
            }
        });
    }

    public void findAllRegistered(Handler<AsyncResult<JsonArray>> resultHandler){
        databaseConnector.query(FINDBYSTATUS_QUERY, Tuple.tuple().addString(BrokerStatus.REGISTERED.toString()), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
            }
        });
    }

    public void findById(long id, Handler<AsyncResult<JsonObject>> resultHandler){
        databaseConnector.query(FINDBYID_QUERY,Tuple.tuple().addLong(id), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            } else {
                resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
            }
        });
    }

}
