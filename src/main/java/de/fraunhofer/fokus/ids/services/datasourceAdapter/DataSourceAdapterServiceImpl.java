package de.fraunhofer.fokus.ids.services.datasourceAdapter;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.sqlclient.Tuple;

import java.util.UUID;

import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataSourceAdapterServiceImpl implements DataSourceAdapterService {

	private Logger LOGGER = LoggerFactory.getLogger(DataSourceAdapterServiceImpl.class.getName());

	private WebClient webClient;
	private Vertx vertx;
	private String tempFileRootPath;
	private String apikey;
	private DatabaseConnector databaseConnector;
	private static final String FINDBYNAME_QUERY = "SELECT host, port FROM adapters WHERE name= $1";

	public DataSourceAdapterServiceImpl(Vertx vertx, WebClient webClient, String tempFileRootPath,
			Handler<AsyncResult<DataSourceAdapterService>> readyHandler) {
		this.webClient = webClient;
		this.databaseConnector = DatabaseConnector.getInstance();
		this.tempFileRootPath = tempFileRootPath;
		this.vertx = vertx;
		
		//  ConfigStoreOptions confStore = new ConfigStoreOptions()
	    //             .setType("env");

	    //     ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

	        ConfigRetriever retriever = ConfigRetriever.create(vertx);

	        retriever.getConfig(ar -> {
	            if (ar.succeeded()) {
	            	apikey = ar.result().getString("API_KEY");
	            } else {
	                LOGGER.error(ar.cause());
	            }
	        });

		readyHandler.handle(Future.succeededFuture(this));
	}

	private void post(int port, String host, String path, JsonObject payload,
			Handler<AsyncResult<JsonObject>> resultHandler) {
		webClient.post(port, host, path).sendJsonObject(payload, ar -> {
			if (ar.succeeded()) {
				LOGGER.debug(ar.result().bodyAsString());
				resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
			} else {
				LOGGER.error(ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	private void download(int port, String host, String path, JsonObject payload,
			Handler<AsyncResult<String>> resultHandler) {
		String fileName = tempFileRootPath + UUID.randomUUID().toString();
		AsyncFile asyncFile = vertx.fileSystem().openBlocking(fileName, new OpenOptions());
		webClient.post(port, host, path).as(BodyCodec.pipe(asyncFile)).sendJsonObject(payload, ar -> {
			if (ar.succeeded()) {
				resultHandler.handle(Future.succeededFuture(fileName));
			} else {
				LOGGER.error(ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	private void getAdapters(String name, Handler<AsyncResult<JsonObject>> resultHandler) {

		databaseConnector.query(FINDBYNAME_QUERY, Tuple.tuple().addString(name), reply -> {
			if (reply.succeeded()) {
				try {
					JsonObject jsonObject = new JsonObject().put("host", reply.result().get(0).getString("host"))
							.put("port", reply.result().get(0).getLong("port"));
					resultHandler.handle(Future.succeededFuture(jsonObject));
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

	private void get(int port, String host, String path, Handler<AsyncResult<JsonObject>> resultHandler) {

		webClient.get(port, host, path).bearerTokenAuthentication(apikey).send(ar -> {
			if (ar.succeeded()) {
				LOGGER.debug(ar.result().bodyAsString());
				resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
			} else {
				LOGGER.error(ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});


	}

	@Override
	public DataSourceAdapterService getFile(String dataSourceType, JsonObject request,
			Handler<AsyncResult<String>> resultHandler) {
		getAdapters(dataSourceType, reply -> {
			if (reply.succeeded()) {
				LOGGER.debug("Port: " + reply.result().getInteger("port"));
				LOGGER.debug("Host: " + reply.result().getString("host"));
				download(reply.result().getInteger("port"), reply.result().getString("host"), "/getFile/", request,
						adapterReply -> {
							if (adapterReply.succeeded()) {
								resultHandler.handle(Future.succeededFuture(adapterReply.result()));
							} else {
								LOGGER.error(adapterReply.cause());
								resultHandler.handle(Future.failedFuture(adapterReply.cause()));
							}
						});
			} else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
		return this;
	}

	@Override
	public DataSourceAdapterService supported(String dataSourceType, Handler<AsyncResult<JsonObject>> resultHandler) {
		getAdapters(dataSourceType, reply -> {
			if (reply.succeeded()) {
				LOGGER.debug("Port: " + reply.result().getInteger("port"));
				LOGGER.debug("Host: " + reply.result().getString("host"));
				get(reply.result().getInteger("port"), reply.result().getString("host"), "/supported/",
						adapterReply -> {
							if (adapterReply.succeeded()) {
								resultHandler.handle(Future.succeededFuture(adapterReply.result()));
							} else {
								LOGGER.error(adapterReply.cause());
								resultHandler.handle(Future.failedFuture(adapterReply.cause()));
							}
						});
			} else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
		return this;
	}

	@Override
	public DataSourceAdapterService delete(String dataSourceType, String resourceId,
			Handler<AsyncResult<JsonObject>> resultHandler) {
		getAdapters(dataSourceType, reply -> {
			if (reply.succeeded()) {
				LOGGER.debug("Port: " + reply.result().getInteger("port"));
				LOGGER.debug("Host: " + reply.result().getString("host"));
				get(reply.result().getInteger("port"), reply.result().getString("host"), "/delete/" + resourceId,
						adapterReply -> {
							if (adapterReply.succeeded()) {
								resultHandler.handle(Future.succeededFuture(adapterReply.result()));
							} else {
								LOGGER.error(adapterReply.cause());
								resultHandler.handle(Future.failedFuture(adapterReply.cause()));
							}
						});
			} else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
		return this;
	}

	@Override
	public DataSourceAdapterService createDataAsset(String dataSourceType, JsonObject message,
			Handler<AsyncResult<JsonObject>> resultHandler) {

		getAdapters(dataSourceType, reply -> {

			if (reply.succeeded()) {
				LOGGER.debug("Port: " + reply.result().getInteger("port"));
				LOGGER.debug("Host: " + reply.result().getString("host"));
				post(reply.result().getInteger("port"), reply.result().getString("host"), "/create/", message,
						adapterReply -> {
							if (adapterReply.succeeded()) {
								resultHandler.handle(Future.succeededFuture(adapterReply.result()));
							} else {
								LOGGER.error(adapterReply.cause());
								resultHandler.handle(Future.failedFuture(adapterReply.cause()));
							}
						});
			} else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
		return this;
	}
}
