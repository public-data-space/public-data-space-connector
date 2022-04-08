package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.persistence.enums.JobStatus;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Tuple;

import java.util.HashMap;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class JobManager {

	private DatabaseConnector databaseConnector;
	private Logger LOGGER = LoggerFactory.getLogger(JobManager.class.getName());

	private static final String ADD_QUERY = "INSERT INTO job (created_at,updated_at,data,status,sourceid, sourcetype) values (NOW(), NOW(), $1, $2, $3, $4) RETURNING id";
	private static final String FINDALL_QUERY = "SELECT * FROM job";
	private static final String DELETEALL_QUERY = "DELETE FROM job";
	private static final String UPDATESTATUS_QUERY = "UPDATE job SET status = $1, updated_at = NOW() WHERE id = $2";

	public JobManager() {
		databaseConnector = DatabaseConnector.getInstance();
	}

	public void add(DataAssetDescription dataAssetDescription, Handler<AsyncResult<JsonObject>> resultHandler) {
		Tuple params = Tuple.tuple()
				.addValue(new JsonObject((dataAssetDescription.getData().isEmpty() ? new HashMap<>() : dataAssetDescription.getData())).toString())
				.addInteger(JobStatus.CREATED.ordinal())
				.addInteger(dataAssetDescription.getSourceId())
				.addString(dataAssetDescription.getDatasourcetype());

		databaseConnector.query(ADD_QUERY, params, reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
			}
		});
	}

	public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
		databaseConnector.query(FINDALL_QUERY, Tuple.tuple(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(new JsonArray(reply.result())));
			}
		});
	}

	public void deleteAll(Handler<AsyncResult<Void>> resultHandler) {
		databaseConnector.query(DELETEALL_QUERY, Tuple.tuple(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

	public void updateStatus(Long id, JobStatus status, Handler<AsyncResult<Void>> resultHandler) {
		databaseConnector.query(UPDATESTATUS_QUERY, Tuple.tuple().addInteger(status.ordinal()).addLong(id), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}

}
