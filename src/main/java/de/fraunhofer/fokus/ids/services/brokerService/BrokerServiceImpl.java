package de.fraunhofer.fokus.ids.services.brokerService;

import de.fraunhofer.fokus.ids.persistence.managers.BrokerManager;
import de.fraunhofer.fokus.ids.services.ConfigService;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.fokus.ids.utils.models.IDSMessage;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class BrokerServiceImpl implements BrokerService {

    private final Logger LOGGER = LoggerFactory.getLogger(BrokerService.class.getName());

    private BrokerManager brokerManager;
    private IDSService idsService;
    private WebClient webClient;
    private Serializer serializer;
    private ConfigService configService;

    public BrokerServiceImpl(Vertx vertx, WebClient webClient, Handler<AsyncResult<BrokerService>> readyHandler){
        this.brokerManager = new BrokerManager();
        this.idsService = new IDSService(vertx);
        this.webClient = webClient;
        this.serializer = new Serializer();
        this.configService = new ConfigService(vertx);
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public BrokerService subscribe(String url, Handler<AsyncResult<Void>> resultHandler){
        Promise<Connector> connectorPromise = Promise.promise();
        Promise<Message> messagePromise = Promise.promise();
        createAvailableIDSMessages(connectorPromise, messagePromise);

        sendSingle(url, connectorPromise.future(), messagePromise.future(), resultHandler);
        return this;
    }

    @Override
    public BrokerService subscribeAll(Handler<AsyncResult<Void>> resultHandler){
        Promise<Connector> connectorPromise = Promise.promise();
        Promise<Message> messagePromise = Promise.promise();
        createAvailableIDSMessages(connectorPromise, messagePromise);

        sendMulti(connectorPromise.future(), messagePromise.future(), resultHandler);
        return this;
    }

    private void createAvailableIDSMessages(Promise<Connector> connectorPromise, Promise<Message> messagePromise) {
        configService.getConfiguration(reply -> {
            if(reply.succeeded()) {
                idsService.getConnector(reply.result(), connectorPromise);
                idsService.createRegistrationMessage(reply.result(), messagePromise);
            } else {
                LOGGER.info(reply.cause());
                connectorPromise.fail(reply.cause());
                messagePromise.fail(reply.cause());
            }
        });
    }

    @Override
    public BrokerService unsubscribeAll(Handler<AsyncResult<Void>> resultHandler){
        Promise<Connector> connectorPromise = Promise.promise();
        Promise<Message> messagePromise = Promise.promise();
        createUnavailableIDSMessages(connectorPromise, messagePromise);

        sendMulti(connectorPromise.future(), messagePromise.future(), resultHandler);
        return  this;
    }

    @Override
    public BrokerService unsubscribe(String url, Handler<AsyncResult<Void>> resultHandler){
        Promise<Connector> connectorPromise = Promise.promise();
        Promise<Message> messagePromise = Promise.promise();
        createUnavailableIDSMessages(connectorPromise, messagePromise);

        sendSingle(url, connectorPromise.future(), messagePromise.future(), resultHandler);
        return this;
    }

    private void createUnavailableIDSMessages(Promise<Connector> connectorPromise, Promise<Message> messagePromise){
        configService.getConfiguration(reply -> {
            if(reply.succeeded()) {
                idsService.getConnector(reply.result(), connectorPromise);
                idsService.createUnregistrationMessage(reply.result(), messagePromise);
            } else {
                LOGGER.info(reply.cause());
                connectorPromise.fail(reply.cause());
                messagePromise.fail(reply.cause());
            }
        });
    }

    @Override
    public BrokerService update(Handler<AsyncResult<Void>> resultHandler){
        Promise<Connector> connectorPromise = Promise.promise();
        Promise<Message> messagePromise = Promise.promise();
        createUpdateIDSMessages(connectorPromise, messagePromise);

        sendMulti(connectorPromise.future(), messagePromise.future(), resultHandler);
        return this;
    }

    private void createUpdateIDSMessages(Promise<Connector> connectorPromise, Promise<Message> messagePromise) {
        configService.getConfiguration(reply -> {
            if(reply.succeeded()) {
                idsService.getConnector(reply.result(), connectorPromise);
                idsService.createUpdateMessage(reply.result(), messagePromise);
            } else {
                LOGGER.info(reply.cause());
                connectorPromise.fail(reply.cause());
                messagePromise.fail(reply.cause());
            }
        });
    }

    private void sendSingle(String url, Future<Connector> connectorFuture, Future messageFuture, Handler<AsyncResult<Void>> resultHandler){
        CompositeFuture.all(connectorFuture, messageFuture).onComplete(reply -> {
            if(reply.succeeded()) {
                List<URL> urls = new ArrayList<>();
                try {
                    urls.add(new URL(url));
                    sendMessage(createBrokerMessage((ConnectorNotificationMessage) messageFuture.result(), connectorFuture.result()), urls, resultHandler);
                } catch (MalformedURLException e) {
                    LOGGER.error(e);
                    resultHandler.handle(Future.failedFuture(e));
                }
            }
            else{
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void sendMulti(Future<Connector> connectorFuture, Future messageFuture, Handler<AsyncResult<Void>> resultHandler){
        CompositeFuture.all(connectorFuture, messageFuture).onComplete(reply -> {
            if(reply.succeeded()){
                getBrokerURLs(reply2 -> {
                    if (reply2.succeeded()) {
                        if(!reply2.result().isEmpty()) {
                            sendMessage(createBrokerMessage((ConnectorNotificationMessage) messageFuture.result(), connectorFuture.result()), reply2.result(), resultHandler);
                        }
                        else {
                            resultHandler.handle(Future.succeededFuture());
                        }
                    } else{
                        LOGGER.error(reply2.cause());
                        resultHandler.handle(Future.failedFuture(reply2.cause()));
                    }
                });
            }
            else{
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void sendMessage(HttpEntity buffer, List<URL> urls, Handler<AsyncResult<Void>> resultHandler){
        if(buffer != null) {
            if(urls.isEmpty()){
                resultHandler.handle(Future.succeededFuture());
            } else {
                try(ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    Header contentTypeHeader = buffer.getContentType();
                    buffer.writeTo(baos);
                    Buffer brokerMessage = Buffer.buffer().appendString(baos.toString());
                    for (URL url : urls) {
                        webClient
                                .postAbs(url.toString())
                                .putHeader(contentTypeHeader.getName(), contentTypeHeader.getValue())
                                .sendBuffer(brokerMessage, ar -> {
                                    if (ar.succeeded()) {
                                        Optional<IDSMessage> answer = IDSMessageParser.parse(ar.result().headers().get(HttpHeaders.CONTENT_TYPE), ar.result().bodyAsString());
                                        if (answer.isPresent() && answer.get().getHeader().isPresent()) {
                                            if (answer.get().getHeader().get() instanceof RejectionMessage) {
                                                resultHandler.handle(Future.failedFuture(((RejectionMessage) answer.get().getHeader().get()).getRejectionReason().toString()));
                                            } else {
                                                resultHandler.handle(Future.succeededFuture());
                                            }
                                        } else {
                                            resultHandler.handle(Future.succeededFuture());
                                        }
                                    } else {
                                        LOGGER.error(ar.cause());
                                        resultHandler.handle(Future.failedFuture(ar.cause()));
                                    }
                                });
                    }
                } catch (IOException e) {
                    LOGGER.error(e);
                    resultHandler.handle(Future.failedFuture(e));
                }
            }
        }
        else{
            LOGGER.error("Message could not be created.");
            resultHandler.handle(Future.failedFuture("Message could not be created."));
        }
    }

    private void getBrokerURLs(Handler<AsyncResult<List<URL>>> resultHandler){
        brokerManager.findAllRegistered( reply -> {
            if (reply.succeeded()){
                List<URL> brokerUrls = new ArrayList<>();
                for(int i =0;i<reply.result().size();i++) {
                    try {
                        brokerUrls.add(new URL(reply.result().getJsonObject(i).getString("url")));
                    } catch (MalformedURLException e) {
                        LOGGER.error(e);
                        break;
                    }
                }
                resultHandler.handle(Future.succeededFuture(brokerUrls));
            }
            else {
                resultHandler.handle(Future.succeededFuture(new ArrayList<>()));
            }
        });
    }

    private HttpEntity createBrokerMessage(ConnectorNotificationMessage message, Connector connector){

        try{
            ContentBody result = new StringBody(serializer.serialize(connector), ContentType.create("application/json", StandardCharsets.UTF_8));
            ContentBody cb = new StringBody(serializer.serialize(message), ContentType.create("application/json", StandardCharsets.UTF_8));

            MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
                    .setCharset(StandardCharsets.UTF_8)
                    .setContentType(ContentType.MULTIPART_FORM_DATA)
                    .addPart("header", cb)
                    .addPart("payload", result);

            return multipartEntityBuilder.build();
        } catch (Exception e){
            LOGGER.error(e);
        }
        return null;
    }
}
