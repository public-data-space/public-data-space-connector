package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.persistence.managers.JobManager;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class JobController extends AbstractVerticle {

	private Logger LOGGER = LoggerFactory.getLogger(JobController.class.getName());
	private JobManager jobManager;

	public JobController(Vertx vertx){
		this.jobManager = new JobManager();
	}

    public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
		jobManager.findAll(reply -> {
			if (reply.succeeded()) {
				resultHandler.handle(Future.succeededFuture(reply.result()));
			}
			else {
				LOGGER.error("DataAsset could not be read.",reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
    }

    public void deleteAll(Handler<AsyncResult<JsonObject>> resultHandler) {
		jobManager.deleteAll(reply -> {
    		if(reply.succeeded()){
				JsonObject jO = new JsonObject();
				jO.put("status", "success");
				jO.put("text", "Alle Jobs wurden gelöscht!");
				resultHandler.handle(Future.succeededFuture(jO));
			}
    		else{
				LOGGER.error("Jobs konnten nicht gelöscht werden!", reply.cause());
				JsonObject jO = new JsonObject();
				jO.put("status", "error");
				jO.put("text", "Jobs konnten nicht gelöscht werden!");
				resultHandler.handle(Future.succeededFuture(jO));
			}
		});
    }
}
