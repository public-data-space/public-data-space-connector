package de.fraunhofer.fokus.ids.controllers;

import java.time.LocalDate;
import java.util.Date;

import de.fraunhofer.fokus.ids.main.ApplicationConfig;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import de.fraunhofer.fokus.ids.services.database.DatabaseService;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;
import io.vertx.sqlclient.Tuple;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataSourceController {

	private Logger LOGGER = LoggerFactory.getLogger(DataSourceController.class.getName());
	private DataSourceManager dataSourceManager;
	private DatabaseConnector databaseConnector;
	private WebClient webClient;
	private String apikey;
	private static final String LISTADAPTERS_QUERY = "SELECT name FROM adapters";
	private static final String FINDBYNAME_QUERY = "SELECT host, port FROM adapters WHERE name= $1";
	private static final String ADD_QUERY = "INSERT INTO adapters (created_at, updated_at, name, host, port) values(NOW(), NOW(), $1, $2, $3)";
	private static final String EDIT_QUERY = "UPDATE adapters SET updated_at = NOW(), host = $1, port = $2 WHERE name = $3 ";

	public DataSourceController(Vertx vertx) {
		this.dataSourceManager = new DataSourceManager();
		this.databaseConnector = DatabaseConnector.getInstance();
		this.webClient = WebClient.create(vertx);
		// ConfigStoreOptions confStore = new ConfigStoreOptions()
        //         .setType("env");

        // ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx);
        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                JsonObject env = ar.result();
                apikey = env.getString(ApplicationConfig.ENV_API_KEY, ApplicationConfig.DEFAULT_API_KEY);
                
            } else {
                LOGGER.error("Config could not be retrieved.");
            }
        });
	}

	public void add(DataSource dataSource, Handler<AsyncResult<JsonObject>> resultHandler) {
		if(dataSource.getDatasourceName() == null || dataSource.getDatasourceName().isEmpty()) {
            JsonObject jO = new JsonObject();
            jO.put("status", "error");
            jO.put("text", "Bitte geben Sie einen DataSourcenamen ein!");
            resultHandler.handle(Future.succeededFuture(jO));
        }
        else if(dataSource.getDatasourceType() == null || dataSource.getDatasourceType() == null) {
            JsonObject jO = new JsonObject();
            jO.put("status", "error");
            jO.put("text", "Bitte geben Sie einen DataSourcetyp ein!");
            resultHandler.handle(Future.succeededFuture(jO));
        }
        else {
            dataSourceManager.add(dataSource, reply -> {
                if(reply.succeeded()) {
                    JsonObject jO = new JsonObject();
                    jO.put("status", "success");
                    jO.put("text", "DataSource wurde erstellt");
                    resultHandler.handle(Future.succeededFuture(jO));
                }
                else {
                    LOGGER.error("DataSource konnte nicht erstellt werden!", reply.cause());
                    JsonObject jO = new JsonObject();
                    jO.put("status", "error");
                    jO.put("text", "DataSource konnte nicht erstellt werden!");
                    resultHandler.handle(Future.succeededFuture(jO));
                }
            });
        }


	}

	public void registerAdapter(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> resultHandler) {
		databaseConnector.query(FINDBYNAME_QUERY, Tuple.tuple().addString(jsonObject.getString("name")), reply -> {
			if (reply.succeeded()) {
				if (reply.result().size() > 0) {
					edit(jsonObject.getString("name"), jsonObject.getJsonObject("address"), reply2 -> {
						if (reply2.succeeded()) {
							JsonObject jO = new JsonObject();
							jO.put("status", "success");
							jO.put("text", "Adapter wurde registriert");
							resultHandler.handle(Future.succeededFuture(jO));
						} else {
							JsonObject jO = new JsonObject();
							jO.put("status", "error");
							jO.put("text", "Der Adapter konnte nicht registriert werden!");
							resultHandler.handle(Future.succeededFuture(jO));
						}
					});
				} else {
					databaseConnector.query(ADD_QUERY, Tuple.tuple().addString(jsonObject.getString("name"))
									.addString(jsonObject.getJsonObject("address").getString("host"))
									.addInteger(jsonObject.getJsonObject("address").getInteger("port")),
							reply3 -> {
								if (reply3.succeeded()) {
									JsonObject jO = new JsonObject();
									jO.put("status", "success");
									jO.put("text", "Adapter wurde registriert");
									resultHandler.handle(Future.succeededFuture(jO));
								} else {
									JsonObject jO = new JsonObject();
									jO.put("status", "error");
									jO.put("text", "Der Adapter konnte nicht registriert werden!");
									resultHandler.handle(Future.succeededFuture(jO));
								}
							});
				}
			}
		});
	}
	
	
	 private void edit(String name, JsonObject jsonObject, Handler<AsyncResult<JsonObject>> resultHandler ){
         databaseConnector.query(EDIT_QUERY, Tuple.tuple().addString(jsonObject.getString("host")).addLong(jsonObject.getLong("port")).addString(name), reply -> {
             if (reply.succeeded()) {
                 JsonObject jO = new JsonObject();
                 jO.put("status", "success");
                 jO.put("text", "Adapter wurde geändert.");
                 resultHandler.handle(Future.succeededFuture(jO));
             } else {
                 JsonObject jO = new JsonObject();
                 jO.put("status", "error");
                 jO.put("text", "Der Adapter konnte nicht geändert werden!");
                 resultHandler.handle(Future.succeededFuture(jO));
             }
         });
 }


	public void listAdapters(Handler<AsyncResult<JsonArray>> resultHandler) {
		databaseConnector.query(LISTADAPTERS_QUERY, Tuple.tuple(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
			}
		});

	}

	public void delete(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataSourceManager.delete(id, reply -> {
			if (reply.succeeded()) {
				JsonObject jO = new JsonObject();
				jO.put("status", "success");
				jO.put("text", "DataSource wurde gelöscht.");
				resultHandler.handle(Future.succeededFuture(jO));
			} else {
				LOGGER.error("DataSource konnte nicht gelöscht werden!", reply.cause());
				JsonObject jO = new JsonObject();
				jO.put("status", "error");
				jO.put("text", "DataSource konnte nicht gelöscht werden!");
				resultHandler.handle(Future.succeededFuture(jO));
			}
		});
	}

	public void update(DataSource dataSource, Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataSource.setId(id);
		dataSourceManager.update(dataSource, reply -> {
			if (reply.succeeded()) {
				JsonObject jO = new JsonObject();
				jO.put("status", "success");
				jO.put("text", "DataSource wurde geändert.");
				resultHandler.handle(Future.succeededFuture(jO));
			} else {
				LOGGER.error("DataSource konnte nicht geändert werden!", reply.cause());
				JsonObject jO = new JsonObject();
				jO.put("status", "error");
				jO.put("text", "DataSource konnte nicht geändert werden!");
				resultHandler.handle(Future.succeededFuture(jO));
			}
		});
	}

	public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
		dataSourceManager.findAll(reply -> {
			if (reply.succeeded()) {
				resultHandler.handle(Future.succeededFuture(reply.result()));
			} else {
				LOGGER.error("DataSources not found.", reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	public void findAllByType(Handler<AsyncResult<JsonObject>> resultHandler) {
		dataSourceManager.findAllByType(reply -> {
			if (reply.succeeded()) {
				resultHandler.handle(Future.succeededFuture(reply.result()));
			} else {
				LOGGER.error("DataSources not found.", reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	public void findByType(String type, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataSourceManager.findByType(type, reply -> {
			if (reply.succeeded()) {
				JsonObject result = new JsonObject().put("type", type).put("result", reply.result());
				resultHandler.handle(Future.succeededFuture(result));
			} else {
				LOGGER.error("DataSources not found.", reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}
	

    public void getFormSchema(String name, Handler<AsyncResult<JsonObject>> resultHandler) {

    	
    	databaseConnector.query(FINDBYNAME_QUERY, Tuple.tuple().addString(name), reply -> {
            if (reply.succeeded()) {
                try {
                    
                    String host = reply.result().get(0).getString("host");
                    int port = reply.result().get(0).getInteger("port");
                    webClient
                    .get(port, host, "/getDataSourceFormSchema/")
                    .bearerTokenAuthentication(apikey)
                    .send(adapterReply -> {
                        if (adapterReply.succeeded()) {
                            resultHandler.handle(Future.succeededFuture(adapterReply.result().bodyAsJsonObject()));
                        } else {
                            LOGGER.error(adapterReply.cause());
                            resultHandler.handle(Future.failedFuture(adapterReply.cause()));
                        }
                    });

    
                } catch (IndexOutOfBoundsException e) {
                    LOGGER.info("Queried adapter not registered.");
                    resultHandler.handle(Future.failedFuture("Queried adapter not registered."));
                }
            } else {
                LOGGER.error("Information for " + name + " could not be retrieved.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
            
        
        });

    }
    public void findById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
    	
    	dataSourceManager.findById(id, reply -> {
            if (reply.succeeded()) {
            	databaseConnector.query(FINDBYNAME_QUERY, Tuple.tuple().addString(reply.result().getString("datasourcetype")), reply2 -> {
                    if (reply2.succeeded()) {
                        try {
                            String host = reply2.result().get(0).getString("host");
                            int port = reply2.result().get(0).getInteger("port");
                            LOGGER.info(host);
                            webClient
                            .get(port, host, "/getDataAssetFormSchema/")
                            .bearerTokenAuthentication(apikey)
                            .send(adapterReply -> {
                                if (adapterReply.succeeded()) {
                                	JsonObject newjO = new JsonObject()
                                            .put("source", reply.result())
                                            .put("formSchema", adapterReply.result().bodyAsJsonObject());
                                	 resultHandler.handle(Future.succeededFuture(newjO));
                                } else {
                                    LOGGER.error(adapterReply.cause());
                                    resultHandler.handle(Future.failedFuture(adapterReply.cause()));
                                }
                            });
                        } catch (IndexOutOfBoundsException e) {
                            LOGGER.info("Queried adapter not registered.");
                            resultHandler.handle(Future.failedFuture("Queried adapter not registered."));
                        }
                    } else {
                        LOGGER.error("Information for " + reply.result().getString("datasourcetype") + " could not be retrieved.", reply2.cause());
                        resultHandler.handle(Future.failedFuture(reply2.cause()));
                    }
                
            	}); 
            }
            else {
                LOGGER.error("DataSource not found.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });

    }
    
   

    
    

}
