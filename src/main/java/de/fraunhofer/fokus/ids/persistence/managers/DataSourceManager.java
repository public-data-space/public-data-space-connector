package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Tuple;

import static de.fraunhofer.fokus.ids.persistence.util.Functions.checkNull;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataSourceManager {

    private DatabaseConnector databaseConnector;
    private Logger LOGGER = LoggerFactory.getLogger(DataSourceManager.class.getName());

    private static final String UPDATE_QUERY = "UPDATE DataSource SET updated_at = NOW(), datasourcename = $1, data = $2, datasourcetype = $3 WHERE id = $4";
    private static final String ADD_QUERY = "INSERT INTO DataSource (created_at, updated_at, datasourcename, data, datasourcetype) values (NOW(), NOW(), $1, $2::JSON, $3)";
    private static final String DELETE_QUERY = "DELETE FROM datasource WHERE id = $1";
    private static final String FINDBYTYPE_QUERY = "SELECT * FROM DataSource WHERE datasourcetype = $1";
    private static final String FINDBYID_QUERY = "SELECT * FROM DataSource WHERE id = $1";
    private static final String FINDALL_QUERY ="SELECT * FROM DataSource ORDER BY id DESC";
    private static final String FINDTYPEBYID_QUERY = "SELECT datasourcetype FROM DataSource WHERE id = $1";
    private static final String FINDALLBYTYPE_QUERY = "SELECT * FROM DataSource ORDER BY datasourcetype";

    public DataSourceManager() {
        this.databaseConnector = DatabaseConnector.getInstance();
    }

    public void update(DataSource dataSource, Handler<AsyncResult<Void>> resultHandler) {

        Tuple params = Tuple.tuple()
                .addString(checkNull(dataSource.getDatasourceName()))
                .addString(dataSource.getData().toString())
                .addString(dataSource.getDatasourceType())
                .addLong(dataSource.getId());

        databaseConnector.query(UPDATE_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });

    }

    public void add(DataSource dataSource, Handler<AsyncResult<Void>> resultHandler) {

        Tuple params = Tuple.tuple()
                .addString(checkNull(dataSource.getDatasourceName()))
                .addString(dataSource.getData().toString())
                .addString(dataSource.getDatasourceType());

        databaseConnector.query(ADD_QUERY, params, reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void delete(Long id, Handler<AsyncResult<Void>> resultHandler) {
        databaseConnector.query(DELETE_QUERY,Tuple.tuple().addLong(id), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }

    public void findByType(String type, Handler<AsyncResult<JsonArray>> resultHandler) {
        databaseConnector.query(FINDBYTYPE_QUERY, Tuple.tuple().addString(type), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
            }
        });
    }

    public void findById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        databaseConnector.query(FINDBYID_QUERY,Tuple.tuple().addLong(id), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
            }
        });
    }

    public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
        databaseConnector.query(FINDALL_QUERY ,Tuple.tuple(), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
            }
        });
    }

    public void findTypeById(Long id, Handler<AsyncResult<Long>> resultHandler) {
        databaseConnector.query(FINDTYPEBYID_QUERY, Tuple.tuple().addLong(id), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("datasourcetype")));
            }
        });
    }

    public void findAllByType(Handler<AsyncResult<JsonObject>> resultHandler) {
        databaseConnector.query( FINDALLBYTYPE_QUERY ,Tuple.tuple(), reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {

                JsonObject res = new JsonObject();
                for(JsonObject jsonObject : reply.result()){
                    if(!res.containsKey(jsonObject.getString("datasourcetype"))){
                        res.put(jsonObject.getString("datasourcetype"), new JsonArray());
                    }
                    res.getJsonArray(jsonObject.getString("datasourcetype")).add(jsonObject);
                }
                resultHandler.handle(Future.succeededFuture(res));
            }
        });
    }

}