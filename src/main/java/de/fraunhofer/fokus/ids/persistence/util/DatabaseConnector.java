package de.fraunhofer.fokus.ids.persistence.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.*;

import java.util.ArrayList;
import java.util.List;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 *
 * @newest_changeses_and_notes_of_Zead:
 *      @properties:
 *          @CREATE_PRIMARY_KEY, @ADD_PRIMARY_KEY, @ADD_FOREIGN_KEY and @FOREIGN_KEY_RULES added.
 *      @methods: (#some_key is a key of the adjustment that you can search for.)
 *          @initTable: (edited)
 *              #Create_Primary_key:
 *                  I create here a primary key if this information is provided in the corresponding table.
 *                  number 1 in the query is replaced with the column name that should be the primary key.
 *
 *              #Extract_none_columns_keys:
 *                  In InitService class we create there objects of tables information. these objects contain info
 *                  about columns names, primary and foreign keys. the method initTable assumes that this objects
 *                  contain just columns names and add this names to a variable named key. So I have not allowed
 *                  primary and foreign keys information to be added in the variable key.
 *
 *          @addColumns: (edited)
 *              #Add_primary_key:
 *                  I do here the same thing as #Create_Primary_key but the key in query is added and not created.
 *
 *          @createAddForeignKeys: (new method)
 *              #createAddForeignKeys:
 *                  I add/create there foreign keys.
 *                  I assume there that the given table should have a foreign key and its object in initService contains
 *                  that Information.
 *
 *         @getForeignKeyStatement: (new method)
 *              #getForeignKeyStatement:
 *                  this method builds the query to add a foreign key
 */
public class DatabaseConnector {

    private Logger LOGGER = LoggerFactory.getLogger(DatabaseConnector.class.getName());
    private PgPool client;
    private RowTransformer rowTransformer;
    private static final DatabaseConnector DBC = new DatabaseConnector();

    private final String CREATE_PRIMARY_KEY = "PRIMARY KEY(1)";
    private final String ADD_PRIMARY_KEY = "ADD PRIMARY KEY(1)";
    private final String ADD_FOREIGN_KEY = "ALTER TABLE 1 DROP CONSTRAINT IF EXISTS 2,ADD CONSTRAINT 2 FOREIGN KEY(3) REFERENCES 4(5)";
    private final String FOREIGN_KEY_RULES = "ON DELETE SET NULL ON UPDATE CASCADE";

    private DatabaseConnector() {
        this.rowTransformer = new RowTransformer();
    }
    public static DatabaseConnector getInstance() {
        return DBC;
    }

    public void create(Vertx vertx, JsonObject config, int maxPoolSize){
        if(client == null) {
            PgConnectOptions connectOptions = new PgConnectOptions()
                    .setPort(config.getInteger("port"))
                    .setHost(config.getString("host"))
                    .setDatabase(config.getString("database"))
                    .setUser(config.getString("user"))
                    .setPassword(config.getString("password"));

            PoolOptions poolOptions = new PoolOptions()
                    .setMaxSize(maxPoolSize);

            this.client = PgPool.pool(vertx, connectOptions, poolOptions);
        } else {
            LOGGER.info("Client already initialized.");
        }
    }

    public void query(String query, Tuple params, Handler<AsyncResult<List<JsonObject>>> resultHandler){
        client.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SqlConnection conn = ar1.result();
                conn.preparedQuery(query)
                        .execute(params, ar2 -> {
                            if (ar2.succeeded()) {
                                resultHandler.handle(Future.succeededFuture(rowTransformer.transform(ar2.result())));
                                conn.close();
                            } else {
                                conn.close();
                                LOGGER.error(ar2.cause());
                                resultHandler.handle(Future.failedFuture(ar2.cause()));
                            }
                        });
            } else {
                LOGGER.error(ar1.cause());
                resultHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    public void initTable(JsonObject columInfo,
                                 String tableName,
                                 Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        List<String> keys = new ArrayList<>();
        //#Extract_none_columns_keys
        columInfo.forEach(e -> {
            if(!e.getKey().equals("primary_key") && !e.getKey().equals("foreign_key") && !e.getKey().equals("ref_key") && !e.getKey().equals("ref_table"))
                keys.add(e.getKey());
        });
            client.getConnection( ar -> {
                if(ar.succeeded()){
                    SqlConnection conn = ar.result();
                    conn.query("select * from " + tableName).execute( r -> {
                        if(r.succeeded()){
                            RowSet<Row> rowSet = r.result();
                            List<String> columns =  new ArrayList(rowSet.columnsNames());
                            List<String> strings = new ArrayList<>();
                            String dropColumns = "";
                            for (String set : rowSet.columnsNames()) {
                                if (!keys.contains(set)) {
                                    dropColumns += "DROP COLUMN " + set + ",";
                                    strings.add(set);
                                }
                            }
                            columns.removeAll(strings);
                            if (!dropColumns.isEmpty()) {
                                String dropQuery = "ALTER TABLE " + tableName + " " + dropColumns;
                                conn.query(dropQuery.substring(0, dropQuery.length() - 1)).execute(delete -> {
                                    if (delete.succeeded()) {
                                        LOGGER.info("Deleted columns " + strings.toString());
                                        addColumns(conn,tableName,keys,columInfo,resultHandler);
                                    } else {
                                        LOGGER.info("Delete columns " + strings.toString() + " failed!");
                                    }
                                });
                            }
                            else {
                                addColumns(conn,tableName,keys,columInfo,resultHandler);
                            }

                        } else {
                            String query = "CREATE TABLE IF NOT EXISTS " + tableName + " (";
                            String columns  ="";

                            for (String key:keys){
                                columns = columns + key + " " + columInfo.getString(key) + ",";
                            }
                            columns = columns.substring(0, columns.length()-1);
                            //#Create_Primary_key
                            query = query +
                                    columns +
                                    (columInfo.containsKey("primary_key")
                                            ?"," + CREATE_PRIMARY_KEY.replace("1", columInfo.getString("primary_key")) + ")"
                                            :")");

                            conn.query(query).execute(resultAsyncResult -> {
                                if (resultAsyncResult.succeeded()) {
                                    resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
                                    conn.close();
                                } else {
                                    LOGGER.error("Update failed.", resultAsyncResult.cause());
                                    resultHandler.handle(Future.failedFuture(resultAsyncResult.cause()));
                                    conn.close();
                                }
                            });

                        }
                    });
                } else {
                    LOGGER.error(ar);
                    resultHandler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    private void addColumns(SqlConnection connection,String tableName,List<String> columns,JsonObject query,Handler<AsyncResult<List<JsonObject>>> resultHandler){
        String updateQuery = "ALTER TABLE " + tableName + " ";
        for (String key : columns) {
            updateQuery += "ADD COLUMN IF NOT EXISTS " + key + " " + query.getString(key) + ",";
        }
        updateQuery = updateQuery.substring(0, updateQuery.length()-1);
        //#Add_primary_key
        updateQuery += (query.containsKey("primary_key")
                ?"," + ADD_PRIMARY_KEY.replace("1", query.getString("primary_key"))
                :"");

        connection.query(updateQuery).execute(add -> {
            if (add.succeeded()) {
                LOGGER.info("Columns added");
                resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
                connection.close();
            } else {
                LOGGER.info("Add failed: " + add.cause());
                resultHandler.handle(Future.failedFuture(add.cause()));
                connection.close();
            }
        });
    }

    public void createAddForeignKeys(String table, JsonObject tableInfo, Handler<AsyncResult<List<JsonObject>>> resultHandler){
        client.getConnection(conn ->{
            if(conn.succeeded()){
                String query = getForeignKeyStatement(table, tableInfo);
                conn.result().query(query).execute(re ->{
                    if(re.succeeded()){
                        LOGGER.info("Foreign key added");
                        resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
                        conn.result().close();
                    } else {
                        LOGGER.info("Foreign key adding failed: " + re.cause());
                        resultHandler.handle(Future.failedFuture(re.cause()));
                        conn.result().close();
                    }
                });
            } else {
                LOGGER.error(conn);
                resultHandler.handle(Future.failedFuture(conn.cause()));
            }
        });
    }

    private String getForeignKeyStatement(String tableName, JsonObject tableInfo){
        String foreignKeyStatement = ADD_FOREIGN_KEY;

        foreignKeyStatement = foreignKeyStatement.replace("1", tableName);
        foreignKeyStatement = foreignKeyStatement.replaceAll("2", tableName + "_" +
                tableInfo.getString("foreign_key") + "_" + tableInfo.getString("ref_table"));
        foreignKeyStatement = foreignKeyStatement.replace("3", tableInfo.getString("foreign_key"));
        foreignKeyStatement = foreignKeyStatement.replace("4", tableInfo.getString("ref_table"));
        foreignKeyStatement = foreignKeyStatement.replace("5", tableInfo.getString("ref_key"));
        foreignKeyStatement += " " + FOREIGN_KEY_RULES;

        return foreignKeyStatement;
    }
}
