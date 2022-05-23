package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.main.ApplicationConfig.*;
import de.fraunhofer.fokus.ids.controllers.*;
import de.fraunhofer.fokus.ids.enums.AcceptType;
import de.fraunhofer.fokus.ids.messages.File;
import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.managers.AuthManager;
import de.fraunhofer.fokus.ids.persistence.managers.BrokerManager;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import de.fraunhofer.fokus.ids.services.ConfigService;
import de.fraunhofer.fokus.ids.services.InitService;
import de.fraunhofer.fokus.ids.services.authAdapter.AuthAdapterServiceVerticle;
import de.fraunhofer.fokus.ids.services.brokerService.BrokerServiceVerticle;
import de.fraunhofer.fokus.ids.services.database.DatabaseService;
import de.fraunhofer.fokus.ids.services.database.DatabaseServiceVerticle;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterServiceVerticle;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.iais.eis.ArtifactRequestMessage;
import de.fraunhofer.iais.eis.DescriptionRequestMessage;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.sqlclient.Tuple;

import org.apache.commons.lang3.SystemUtils;
import org.apache.http.entity.ContentType;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class MainVerticle extends AbstractVerticle {
    private Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
    private AuthManager authManager;
    private ConnectorController connectorController;
    private DataAssetController dataAssetController;
    private DataSourceController dataSourceController;
    private JobController jobController;
    private BrokerController brokerController;
    private BrokerManager brokerManager;
    private ConfigService configService;
    private int servicePort;
    private String apikey;
    private DatabaseService databaseService;

    @Override
    public void start(Promise<Void> startPromise) {
        this.authManager = new AuthManager(vertx);
        this.connectorController = new ConnectorController(vertx);
        this.dataAssetController = new DataAssetController(vertx);
        this.dataSourceController = new DataSourceController(vertx);
        this.jobController = new JobController(vertx);
        this.brokerController = new BrokerController(vertx);
        this.brokerManager = new BrokerManager();
        this.configService = new ConfigService(vertx);

        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setWorker(true);

        LOGGER.info("Starting services...");
        Future<String> deployment = Future.succeededFuture();
        deployment
		        .compose(id1 -> {
		        	Promise<String> databaseDeployment = Promise.promise();
		            Future<String> databaseDeploymentFuture = databaseDeployment.future();
		            vertx.deployVerticle(DatabaseServiceVerticle.class.getName(), deploymentOptions, databaseDeploymentFuture);
		            LOGGER.info("Database started");
		            return databaseDeploymentFuture;
		        })
                .compose(id2 -> {
                    Promise<String> datasourceAdapterPromise = Promise.promise();
                    Future<String> datasourceAdapterFuture = datasourceAdapterPromise.future();
                    vertx.deployVerticle(DataSourceAdapterServiceVerticle.class.getName(), deploymentOptions, datasourceAdapterFuture);
                    return datasourceAdapterFuture;
                })
                .compose(i45 -> {
                    Promise<String> authAdapterPromise = Promise.promise();
                    Future<String> authAdapterFuture = authAdapterPromise.future();
                    vertx.deployVerticle(AuthAdapterServiceVerticle.class.getName(), deploymentOptions, authAdapterFuture);
                    return authAdapterFuture;
                })
                .compose(id5 -> {
                    Promise<String> brokerServicePromise = Promise.promise();
                    Future<String> brokerServiceFuture = brokerServicePromise.future();
                    vertx.deployVerticle(BrokerServiceVerticle.class.getName(), deploymentOptions, brokerServiceFuture);
                    return brokerServiceFuture;
                })
                .compose(id6 -> {
                    Promise<String> envPromise = Promise.promise();
                    // ConfigStoreOptions confStore = new ConfigStoreOptions()
                    //         .setType("env");
                    // ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);
                    ConfigRetriever retriever = ConfigRetriever.create(vertx);
                    retriever.getConfig(ar -> {
                        if (ar.succeeded()) {
                            servicePort = ar.result().getInteger(ApplicationConfig.ENV_SERVICE_PORT, ApplicationConfig.DEFAULT_SERVICE_PORT);
                            apikey = ar.result().getString(ApplicationConfig.ENV_API_KEY, ApplicationConfig.DEFAULT_API_KEY);
                            DatabaseConnector.getInstance().create(vertx, ar.result().getJsonObject(ApplicationConfig.ENV_DB_CONFIG, ApplicationConfig.DEFAULT_DB_CONFIG), 5);
                            envPromise.complete();
                        } else {
                            envPromise.fail(ar.cause());
                        }
                    });
                    return envPromise.future();
	                })
                .onComplete(ar -> {
		            if (ar.succeeded()) {
		            	this.databaseService = DatabaseService.createProxy(vertx, ApplicationConfig.DATABASE_SERVICE);
		                InitService initService = new InitService(vertx);
		                initService.initDatabase(reply -> {
		                    if (reply.succeeded()) {
		                        createHttpServer(vertx);
		                        startPromise.complete();
		                    } else {
		                        LOGGER.error(reply.cause());
		                        startPromise.fail(ar.cause());
		                    }
		                });
		            } else {
		                LOGGER.error(ar.cause());
		                startPromise.fail(ar.cause());
		            }
		        });

    }

    private void createHttpServer(Vertx vertx) {
    	
        OpenAPI3RouterFactory.create(vertx, "/webroot/swagger.yaml", ar -> {
            if (ar.succeeded()) {
                OpenAPI3RouterFactory routerFactory = ar.result();

                Set<String> allowedHeaders = new HashSet<>();
                allowedHeaders.add("x-requested-with");
                allowedHeaders.add("Access-Control-Allow-Origin");
                allowedHeaders.add("origin");
                allowedHeaders.add("Content-Type");
                allowedHeaders.add("accept");
                allowedHeaders.add("Authorization");

                Set<HttpMethod> allowedMethods = new HashSet<>();
                allowedMethods.add(HttpMethod.GET);
                allowedMethods.add(HttpMethod.POST);
                allowedMethods.add(HttpMethod.DELETE);

                routerFactory.addGlobalHandler(CorsHandler.create(".*.").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods).allowCredentials(true));

                routerFactory.addSecurityHandler("bearerAuth", JWTAuthHandler.create(authManager.getProvider()));

                routerFactory
                        .addHandlerByOperationId("loginId", routingContext ->
                                authManager.login(routingContext.getBodyAsJson(), reply -> {
                                    if (reply.succeeded()) {
                                        if (reply.result() != null) {
                                            routingContext.response().end(reply.result());
                                        } else {
                                            routingContext.fail(401);
                                        }
                                    } else {
                                        routingContext.response().setStatusCode(500).end();
                                    }
                                })
                        )
                        .addHandlerByOperationId("aboutPostId", routingContext ->
                                connectorController.checkMessage(IDSMessageParser.parse(
                                        routingContext.request().formAttributes()), DescriptionRequestMessage.class, routingContext.response()))
                        .addHandlerByOperationId("aboutGetId", routingContext ->
                                connectorController.about(result ->
                                        reply(result, routingContext.response())))
                        .addHandlerByOperationId("dataPostId", routingContext ->
                                connectorController.checkMessage(IDSMessageParser.parse(
                                        routingContext.request().formAttributes()), ArtifactRequestMessage.class, routingContext.response()))
                        .addHandlerByOperationId("dataGetId", routingContext ->
                                connectorController.payload(true, Long.parseLong(routingContext.request().getParam("id")), "", null
                                        , routingContext.response()))
                        .addHandlerByOperationId("infrastructureId", routingContext ->
                                connectorController.routeMessage(IDSMessageParser.parse(
                                        routingContext.request().formAttributes()), routingContext.response()))
                        // Jobs

                        .addHandlerByOperationId("jobGetId", routingContext ->
                                jobController.findAll(result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("jobDeleteId", routingContext ->
                                jobController.deleteAll(result -> reply(result, routingContext.response())))

                        // Data Assets
                        
                        .addHandlerByOperationId("getDataAssetId", routingContext ->
                        dataAssetController.getById(Long.parseLong(routingContext.request().getParam("id")),result -> reply(result, routingContext.response())))
                        //TODO create Endpoint
                        .addHandlerByOperationId("tagsDataAssetId", routingContext ->
                        dataAssetController.generateTags(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                        

                        .addHandlerByOperationId("getCountsId", routingContext ->
                                dataAssetController.counts(result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("publishAllDataAssetsId", routingContext ->
                                dataAssetController.publishAll(result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("unpublishAllDataAssetsId", routingContext ->
                                dataAssetController.unpublishAll(result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("publishDataAssetId", routingContext ->
                                dataAssetController.publish(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("unpublishDataAssetId", routingContext ->
                                dataAssetController.unPublish(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("deleteDataAssetId", routingContext ->
                                dataAssetController.delete(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("getDataAssetsId", routingContext ->
                                dataAssetController.index(result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("addDataAssetId", this::processDataAssetInformation)

                        
                        // Data Sources

                        .addHandlerByOperationId("dataSourceAddId", routingContext ->
                                dataSourceController.add(toDataSource(routingContext.getBodyAsJson()), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("dataSourceDeleteId", routingContext ->
                                dataSourceController.delete(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("allDataSourceGetId", routingContext ->
                                dataSourceController.findAllByType(result -> reply(result, routingContext.response())))
                        
                        .addHandlerByOperationId("datasourceGetId", routingContext ->
                                dataSourceController.findById(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                           
                        .addHandlerByOperationId("dataSourceTypeGetId", routingContext ->
                                dataSourceController.findByType(routingContext.request().getParam("type"), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("dataSourceEditId", routingContext ->
                                dataSourceController.update(toDataSource(routingContext.getBodyAsJson()), Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                        
                        .addHandlerByOperationId("dataSourceSchemaGetId", routingContext ->
                                dataSourceController.getFormSchema(routingContext.request().getParam("type"), result -> reply(result, routingContext.response())))
                         		
                        // Broker

                        .addHandlerByOperationId("brokerAddId", routingContext ->
                                brokerController.add(routingContext.getBodyAsJson().getString("url"), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("brokerUnregisterId", routingContext ->
                                brokerController.unregister(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("brokerRegisterId", routingContext ->
                                brokerController.register(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("brokerGetId", routingContext ->
                                brokerManager.findAll(result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("brokerDeleteId", routingContext ->
                                brokerController.delete(Long.parseLong(routingContext.request().getParam("id")), result -> reply(result, routingContext.response())))

                        // Config

                        .addHandlerByOperationId("configGetId", routingContext ->
                                configService.getConfiguration(result -> reply(result, routingContext.response())))
                        .addHandlerByOperationId("configEditId", routingContext ->
                                configService.editConfiguration(routingContext.getBodyAsJson(), result -> reply(result, routingContext.response())))

                        // Adapters
                        
                        .addHandlerByOperationId("registerId", routingContext -> 
                        		dataSourceController.registerAdapter(routingContext.getBodyAsJson(), reply -> reply(reply, routingContext.response())))
                        .addHandlerByOperationId("adapterGetId", routingContext ->
                                dataSourceController.listAdapters(result -> reply(result, routingContext.response())))
                        
                    
                   
                ;

                Router router = routerFactory.getRouter();
                router.route("/").handler(routingContext -> {
                    connectorController.about(result ->
                            reply(result, routingContext.response()));
                });
                router.route("/api*").handler(StaticHandler.create());

                HttpServer server = vertx.createHttpServer();
                server.requestHandler(router).listen(this.servicePort);
                LOGGER.info("public-data-space-connector deployed on port " + servicePort);

            } else {
                LOGGER.error(ar.cause());
            }
        });
    }
    
  
    

    private void processDataAssetInformation(RoutingContext routingContext) {
        if (routingContext.parsedHeaders().contentType().value().contains(AcceptType.JSON.getHeader())) {
            JsonObject jsonObject = routingContext.getBodyAsJson();
            String licenseurl = jsonObject.getString("licenseurl");
            String licensetitle = jsonObject.getString("licensetitle");
            jsonObject.remove("licenseurl");
            jsonObject.remove("licensetitle");
            dataAssetController.add(Json.decodeValue(jsonObject.toString(), DataAssetDescription.class), licenseurl, licensetitle, result -> reply(result, routingContext.response()));

        } else {


            if(routingContext.request().getFormAttribute("data")== null){
                routingContext.response().setStatusCode(400).setStatusMessage("No Data in body").end();
            }


            JsonObject jsonObject = new JsonObject(routingContext.request().getFormAttribute("data"));
            String licenseurl = jsonObject.getString("licenseurl", "");
            String licensetitle = jsonObject.getString("licensetitle", "");
            jsonObject.remove("licenseurl");
            jsonObject.remove("licensetitle");




            Set<FileUpload> fileUploadSet = routingContext.fileUploads();
            Set<File> files = new HashSet<>();
            for (FileUpload fileUpload : fileUploadSet) {

                File f = new File();
                // To get the uploaded file do
                Buffer uploadedFile = vertx.fileSystem().readFileBlocking(fileUpload.uploadedFileName());
                f.setBytes(uploadedFile.getBytes());
                // Uploaded File Name
                try {
                    String fileName = URLDecoder.decode(fileUpload.fileName(), "UTF-8");
                    f.setName(fileName);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                f.setCharSet(fileUpload.charSet());
                f.setContentTransferEncoding(fileUpload.contentTransferEncoding());
                f.setSize(fileUpload.size());
                f.setContentType(fileUpload.contentType());
                files.add(f);

            }


            DataAssetDescription dataAssetDescription = Json.decodeValue(jsonObject.toString(), DataAssetDescription.class);
            dataAssetDescription.setFiles(files);

            dataAssetController.add(dataAssetDescription,licenseurl, licensetitle,  result -> reply(result, routingContext.response()));

        }
    }

    //TODO: WORKAROUND. Find way to use Json.deserialize()
    private DataSource toDataSource(JsonObject bodyAsJson) {
        DataSource ds = new DataSource();
        ds.setData(bodyAsJson.getJsonObject("data"));
        ds.setDatasourceName(bodyAsJson.getString("datasourcename"));
        ds.setDatasourceType(bodyAsJson.getString("datasourcetype"));
        return ds;
    }

    private void reply(Object result, HttpServerResponse response) {
        if (result != null) {
            String entity = result.toString();
            response.putHeader("content-type", ContentType.APPLICATION_JSON.toString());
            response.end(entity);
        } else {
            LOGGER.error("Result is null.");
            response.setStatusCode(500).end();
        }
    }

    
    private void reply(AsyncResult result, HttpServerResponse response) {
        if (result.succeeded()) {
            reply(result.result(), response);
        } else {
            LOGGER.error("Result Future failed.", result.cause());
            response.setStatusCode(500).end();
        }
    }


    public static void main(String[] args) {
        String[] params = Arrays.copyOf(args, args.length + 1);
        params[params.length - 1] = MainVerticle.class.getName();
        Launcher.executeCommand("run", params);
    }
}
