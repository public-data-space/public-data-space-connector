package de.fraunhofer.fokus.ids.services.datasourceAdapter;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
@ProxyGen
@VertxGen
public interface DataSourceAdapterService {

    @Fluent
    DataSourceAdapterService getFile(String dataSourceType, JsonObject request, Handler<AsyncResult<String>> resultHandler);

    @Fluent
    DataSourceAdapterService supported(String dataSourceType, Handler<AsyncResult<JsonObject>> resultHandler);

    @Fluent
    DataSourceAdapterService delete(String dataSourceType, String resourceId, Handler<AsyncResult<JsonObject>> resultHandler);

    @Fluent
    DataSourceAdapterService createDataAsset(String dataSourceType, JsonObject message, Handler<AsyncResult<JsonObject>> resultHandler);

    @GenIgnore
    static DataSourceAdapterService create(Vertx vertx, WebClient webClient, String tempFileRootPath, Handler<AsyncResult<DataSourceAdapterService>> readyHandler) {
        return new DataSourceAdapterServiceImpl(vertx, webClient, tempFileRootPath, readyHandler);
    }

    @GenIgnore
    static DataSourceAdapterService createProxy(Vertx vertx, String address) {
        return new DataSourceAdapterServiceVertxEBProxy(vertx, address);
    }

}
