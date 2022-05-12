package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.main.ApplicationConfig;
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
import io.vertx.sqlclient.Tuple;
import org.mindrot.jbcrypt.BCrypt;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class InitService{

	private final Logger LOGGER = LoggerFactory.getLogger(InitService.class.getName());

	private Vertx vertx;

	private final String ADMIN_CREATE_QUERY = "INSERT INTO public.user(created_at, updated_at, username, password) SELECT NOW(), NOW(), $1, $2 WHERE NOT EXISTS ( SELECT 1 FROM public.user WHERE username=$1)";

	private final JsonObject user = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("username","TEXT")
			.put("password","TEXT")
			.put("primary_key", "id");

	private final JsonObject dataset = new JsonObject().put("id","SERIAL").put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP").put("resourceid","TEXT").put("license","TEXT")
			.put("title","TEXT").put("description","TEXT").put("publisher","TEXT").put("status","INTEGER")
			.put("tags","TEXT[]").put("version","TEXT").put("sourceid","BIGINT")
			.put("pid","TEXT").put("author","TEXT").put("data_access_level","TEXT")
			.put("additionalmetadata","JSONB")
			.put("primary_key", "id").put("foreign_key", "sourceid").put("ref_key", "id")
			.put("ref_table", "datasource");

	private final JsonObject distribution = new JsonObject().put("id","SERIAL").put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP").put("resourceid","TEXT").put("license","TEXT")
			.put("title","TEXT").put("description","TEXT").put("publisher","TEXT").put("filename","TEXT").put("filetype","TEXT")
			.put("byte_size","INT")
			.put("datasetid","TEXT").put("additionalmetadata","JSONB")
			.put("primary_key", "id").put("foreign_key", "datasetid").put("ref_key", "id")
			.put("ref_table", "dataset");

	private final JsonObject datasource = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("datasourcename","TEXT")
			.put("data","JSONB")
			.put("datasourcetype","TEXT")
			.put("primary_key", "id");

	private final JsonObject job = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("data","JSONB")
			.put("status","INTEGER")
			.put("sourceid","BIGINT")
			.put("sourcetype","TEXT")
			.put("primary_key", "id").put("foreign_key", "sourceid").put("ref_key", "id")
			.put("ref_table", "datasource");

	private final JsonObject broker = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("url","TEXT")
			.put("status","TEXT")
			.put("primary_key", "id");

	private final JsonObject configuration = new JsonObject().put("id","SERIAL")
			.put("country","TEXT")
			.put("url","TEXT")
			.put("maintainer","TEXT")
			.put("curator","TEXT")
			.put("title","TEXT")
			.put("primary_key", "id");
	
	private final JsonObject adapters = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("name","TEXT")
			.put("host","TEXT")
			.put("port","INTEGER")
			.put("primary_key", "id");
	
	//TODO do we need containers and images in the db??????
	private final JsonObject containers = new JsonObject().put("id","SERIAL")
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("imageId","BIGINT")
			.put("containerId","BIGINT")
			.put("name","TEXT")
			.put("primary_key", "id");
	
	private final JsonObject images = new JsonObject()
			.put("created_at","TIMESTAMP")
			.put("updated_at","TIMESTAMP")
			.put("uuid","BIGINT")
			.put("imageId","BIGINT")
			.put("primary_key", "imageId");
	
	
	public InitService(Vertx vertx){
		this.vertx = vertx;
	}

	public void initDatabase(Handler<AsyncResult<Void>> resultHandler){

		initTables(reply -> {
			if(reply.succeeded()){
				createAdminUser(reply2 -> {
					if (reply2.succeeded()) {
						resultHandler.handle(Future.succeededFuture());
					}
					else{
						LOGGER.error("Initialization failed.", reply2.cause());
						resultHandler.handle(Future.failedFuture(reply2.cause()));
					}
				});

			}
		});
	}

	private Future<List<JsonObject>> performUpdate(JsonObject query,String tablename){
		Promise<List<JsonObject>> queryPromise = Promise.promise();
		Future<List<JsonObject>> queryFuture = queryPromise.future();
		DatabaseConnector.getInstance().initTable(query,tablename, queryFuture);
		return queryFuture;
	}

	private void initTables(Handler<AsyncResult<Void>> resultHandler){

		ArrayList<Future> list = new ArrayList<Future>() {{
            performUpdate(user,"public.user");
            performUpdate(dataset,"dataset");
			performUpdate(distribution,"distribution");
			performUpdate(datasource,"datasource");
            performUpdate(broker,"broker");
            performUpdate(job,"job");
            performUpdate(configuration,"configuration");
            //add Adapters from config manager
            
            performUpdate(adapters,"adapters");
            //add containers and images from service docker 
            performUpdate(containers,"containers");
            performUpdate(images,"images");
		}};

		CompositeFuture.all(list).onComplete( reply -> {
			if(reply.succeeded()) {

				LOGGER.info("Tables creation finished.");
				resultHandler.handle(Future.succeededFuture());
			}
			else{
				LOGGER.error("Tables creation failed", reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	private void createAdminUser(Handler<AsyncResult<Void>> resultHandler){

		// ConfigStoreOptions confStore = new ConfigStoreOptions()
		// 		.setType("env");

		// ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

		ConfigRetriever retriever = ConfigRetriever.create(vertx);

		retriever.getConfig(ar -> {
			if (ar.succeeded()) {
				DatabaseConnector.getInstance().query(ADMIN_CREATE_QUERY, Tuple.tuple()
						.addString(ar.result().getJsonObject(ApplicationConfig.ENV_FRONTEND_CONFIG, ApplicationConfig.DEFAULT_FRONTEND_CONFIG).getString("username"))
						.addString(BCrypt.hashpw(ar.result().getJsonObject(ApplicationConfig.ENV_FRONTEND_CONFIG, ApplicationConfig.DEFAULT_FRONTEND_CONFIG).getString("password"), BCrypt.gensalt()))
						, reply -> {
					if (reply.succeeded()) {
						LOGGER.info("Adminuser created.");
						resultHandler.handle(Future.succeededFuture());
					} else {
						LOGGER.error("Adminuser creation failed.", reply.cause());
						resultHandler.handle(Future.failedFuture(reply.cause()));
					}
				});
			}
			else{
				LOGGER.error("ConfigRetriever failed.", ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}
	
}
