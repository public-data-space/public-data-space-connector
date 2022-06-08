package de.fraunhofer.fokus.ids.services.datasourceAdapter;

import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.sqlclient.Tuple;

public class DataSourceAdapterDownloadService {
    private Logger LOGGER = LoggerFactory.getLogger(DataSourceAdapterDownloadService.class.getName());
    private WebClient webClient;
    private Vertx vertx;
    private DatabaseConnector databaseConnector;
	private static final String FINDBYNAME_QUERY = "SELECT host, port FROM adapters WHERE name= $1";
    
    public DataSourceAdapterDownloadService(Vertx vertx){
        this.vertx = vertx;
        create();
    }
    public void create() {
        this.databaseConnector = DatabaseConnector.getInstance();
        this.webClient = WebClient.create(vertx);
    }

    private void download(int port, String host, String path, JsonObject payload, HttpServerResponse httpServerResponse) {
        httpServerResponse.putHeader("Transfer-Encoding", "chunked");
        httpServerResponse.putHeader(HttpHeaders.CONTENT_DISPOSITION,"attachment; filename=\""
                +payload.getJsonObject("dataAsset").getString("filename")+"\"");

        webClient
                .post(port, host, path)
                .as(BodyCodec.pipe(httpServerResponse))
                .sendJsonObject(payload, ar -> {
                    if (ar.succeeded()) {
                    	LOGGER.info(port + " " + host + " " + path);
                        LOGGER.info("Status Code "+ar.result().statusCode());
                    } else {
                        LOGGER.error(ar.cause());

                    }
                });
    }


    public void getFile(String dataSourceType, JsonObject request, HttpServerResponse httpServerResponse) {
        getAdapter(dataSourceType, reply -> {
            if(reply.succeeded()) {
                download(reply.result().getInteger("port"), reply.result().getString("host"), "/getFile/", request,httpServerResponse);
            } else {
                LOGGER.error(reply.cause());
            }
        });
    }

    public void downloadFile(String dataSourceType, String resourceId, String dataAssetId, String fileName, HttpServerResponse response){
        getAdapter(dataSourceType, result -> {
            if(result.succeeded()){
                int port = result.result().getInteger("port");
                String host = result.result().getString("host");
                JsonObject linkData = new JsonObject()
                        .put("resourceId", resourceId)
                        .put("dataAssetId", dataAssetId)
                        .put("name", fileName);

                response.putHeader("Transfer-Encoding", "chunked")
                        .putHeader("content-type", "multipart/form-data;charset=UTF-8")
                        .putHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");

                webClient.post(port, host, "/getLink")
                        .as(BodyCodec.pipe(response))
                        .sendJsonObject(linkData,
                        adapterReply -> {
                            if (adapterReply.succeeded()) {
                                LOGGER.info("File sent to client. response status code is: " + adapterReply.result().statusCode());
                            } else {
                                LOGGER.error("Some thing went wrong. Message is: " + adapterReply.cause().getMessage());
                            }
                        });
            }
            else{
                this.LOGGER.error("Could not get adapter data from Data base");
            }
        });
    }
    
    private void getAdapter(String name, Handler<AsyncResult<JsonObject>> resultHandler) {

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
}
