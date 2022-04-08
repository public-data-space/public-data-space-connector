package de.fraunhofer.fokus.ids.services.brokerService;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
@ProxyGen
@VertxGen
public interface BrokerService {

    @Fluent
    BrokerService subscribe(String url, Handler<AsyncResult<Void>> readyHandler);

    @Fluent
    BrokerService unsubscribe(String url, Handler<AsyncResult<Void>> readyHandler);

    @Fluent
    BrokerService subscribeAll(Handler<AsyncResult<Void>> readyHandler);

    @Fluent
    BrokerService unsubscribeAll(Handler<AsyncResult<Void>> readyHandler);

    @Fluent
    BrokerService update(Handler<AsyncResult<Void>> readyHandler);

    @GenIgnore
    static BrokerService create(Vertx vertx, WebClient webClient, Handler<AsyncResult<BrokerService>> readyHandler) {
        return new BrokerServiceImpl(vertx, webClient, readyHandler);
    }

    @GenIgnore
    static BrokerService createProxy(Vertx vertx, String address) {
        return new BrokerServiceVertxEBProxy(vertx, address);
    }
}
