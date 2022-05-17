package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.persistence.entities.Dataset;
import de.fraunhofer.fokus.ids.persistence.entities.Distribution;
import de.fraunhofer.fokus.ids.persistence.entities.Resource;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Tuple;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import static de.fraunhofer.fokus.ids.persistence.util.Functions.checkNull;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetManager {

	private Logger LOGGER = LoggerFactory.getLogger(DataAssetManager.class.getName());
	private DatabaseConnector databaseConnector;

	private static final String FINDBYDATASETID_QUERY = "SELECT * FROM Dataset WHERE id = $1";
	private static final String FINDBYDISTRIBUTIONID_QUERY = "SELECT * FROM Distribution WHERE id = $1";
	private static final String FINDBYDATASETRESOURCEID_QUERY = "SELECT * FROM Dataset WHERE resourceid = $1";
	private static final String FINDPUBLISHED_QUERY = "SELECT * FROM Dataset WHERE status = $1";
	private static final String FINDALL_QUERY = "SELECT * FROM Dataset ORDER BY id DESC";
	private static final String COUNT_QUERY = "SELECT COUNT(d) FROM Dataset d";
	private static final String COUNTPUBLISHED_QUERY = "SELECT COUNT(d) FROM Dataset d WHERE d.status = $1";
	private static final String CHANGESTATUS_UPDATE = "UPDATE Dataset SET status = $1, updated_at = NOW() WHERE id = $2";
	private static final String FINDDISTRIBUTIONBYDATASETID_QUERY = "SELECT * FROM Distribution WHERE datasetId = $1";
	private static final String INSERT_DATASET = "INSERT INTO Dataset (created_at, updated_at, resourceid, license, title, description, publisher, status, tags, version, sourceid, pid, author, data_access_level, additionalmetadata) " +
			"VALUES (NOW(), NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)";
	private static final String INSERT_DISTRIBUTION = "INSERT INTO Distribution (created_at, updated_at, resourceid, license, title, description, publisher, filename, filetype, byte_size, datasetid, additionalmetadata) " +
			"VALUES (NOW(), NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

	private static final String DELETE_DAT_UPDATE = "DELETE FROM dataset WHERE id = $1";
	private static final String DELETE_DIST_UPDATE = "DELETE FROM distribution WHERE datasetid = $1";

	public DataAssetManager() {
		databaseConnector = DatabaseConnector.getInstance();
	}

	public void findDatasetById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		querySingleEntry(id, FINDBYDATASETID_QUERY,resultHandler);
	}

	public void findDatasetByResourceId(String resourceId, Handler<AsyncResult<JsonObject>> resultHandler) {
		querySingleEntry(resourceId, FINDBYDATASETRESOURCEID_QUERY,resultHandler);
	}

	public void findDistributionById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		querySingleEntry(id, FINDBYDISTRIBUTIONID_QUERY,resultHandler);
	}

	private void querySingleEntry(Object id, String query, Handler<AsyncResult<JsonObject>> resultHandler){
		databaseConnector.query(query, Tuple.tuple().addValue(id),reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			} else {
				if(reply.result().isEmpty()){
					resultHandler.handle(Future.failedFuture("Id not in database"));
				} else {
					resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
				}
			}
		});
	}

	private void findDatasetList(String query, Tuple tuple, Handler<AsyncResult<JsonArray>> resultHandler){
		databaseConnector.query(query, tuple, reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				JsonArray jA = new JsonArray(reply.result());
				Iterator iterator = jA.iterator();
				List<Future> datasetFutureList = new ArrayList<>();
				while(iterator.hasNext()){
					Promise<Dataset> promise = Promise.promise();
					datasetFutureList.add(promise.future());
					Dataset da = Json.decodeValue(this.buildDataAssetAdditionalData((JsonObject)iterator.next()), Dataset.class);
					buildDataset(da, promise);
				}
				CompositeFuture.all(datasetFutureList).onComplete( ac -> {
					if(ac.succeeded()){
						List<Dataset> datasetList = new ArrayList<>();
						for(Future fu : datasetFutureList){
							datasetList.add((Dataset)fu.result());
						}
						resultHandler.handle(Future.succeededFuture(new JsonArray(Json.encode(datasetList))));
					} else {
						LOGGER.error(ac.cause());
						resultHandler.handle(Future.failedFuture(ac.cause()));
					}
				});
			}
		});
	}

	public void findPublished(Handler<AsyncResult<JsonArray>> resultHandler) {
		findDatasetList(FINDPUBLISHED_QUERY, Tuple.tuple().addInteger(DataAssetStatus.PUBLISHED.ordinal()), resultHandler);
	}

	private void buildDataset(Dataset da, Handler<AsyncResult<Dataset>> next) {
		databaseConnector.query(FINDDISTRIBUTIONBYDATASETID_QUERY, Tuple.tuple().addString(da.getResourceId()), reply2 -> {
			if(reply2.succeeded()){
				Set<Distribution> dists = reply2.result().stream().map(jO ->
						Json.decodeValue(this.buildDistributionAdditionalData(jO), Distribution.class)).collect(Collectors.toSet());
				da.setDistributions(dists);
				next.handle(Future.succeededFuture(da));
			} else {
				next.handle(Future.failedFuture(reply2.cause()));
			}
		});
	}

	private String buildDataAssetAdditionalData(JsonObject dataAsset){
		String[] keys = {"pid", "author", "data_access_level"};
		for(String s : keys){
			if(dataAsset.containsKey(s)) {
				JsonArray value = new JsonArray();
				value.add(dataAsset.getString(s));
				dataAsset.getJsonObject("additionalmetadata").put(s, value);
				dataAsset.remove(s);
			}
		}
		return dataAsset.toString();
	}

	private String buildDistributionAdditionalData(JsonObject distribution){
		String additionalDataKey = "byte_size";
		if(distribution.containsKey(additionalDataKey)) {
			JsonArray value = new JsonArray();
			value.add(distribution.getInteger(additionalDataKey).toString());
			distribution.getJsonObject("additionalmetadata").put(additionalDataKey, value);
			distribution.remove(additionalDataKey);
		}
		return distribution.toString();
	}

	public void findAll(Handler<AsyncResult<JsonArray>> resultHandler) {
		findDatasetList(FINDALL_QUERY, Tuple.tuple(), resultHandler);
	}

	public void count(Handler<AsyncResult<Long>> resultHandler) {
		databaseConnector.query(COUNT_QUERY, Tuple.tuple(), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("count")));
			}
		});
	}

	public void countPublished(Handler<AsyncResult<Long>> resultHandler) {
		databaseConnector.query(COUNTPUBLISHED_QUERY,Tuple.tuple().addInteger(DataAssetStatus.PUBLISHED.ordinal()), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture(reply.result().get(0).getLong("count")));
			}
		});
	}

	public void changeStatus(DataAssetStatus status, Long id, Handler<AsyncResult<Void>> resultHandler) {
		databaseConnector.query(CHANGESTATUS_UPDATE, Tuple.tuple().addInteger(status.ordinal()).addLong(id), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				resultHandler.handle(Future.succeededFuture());
			}
		});
	}


	public void add(JsonObject dataAssetJson, Handler<AsyncResult<Void>> resultHandler) {

		Dataset dataAsset = Json.decodeValue(dataAssetJson.toString(),Dataset.class);
		JsonObject dataAssetAdditionalData = processAdditionalMetadata(dataAsset);

		Tuple datasetParams = Tuple.tuple()
				.addString(checkNull(dataAsset.getResourceId()))
				.addString(checkNull(dataAsset.getLicense()))
				.addString(checkNull(dataAsset.getTitle()))
				.addString(checkNull(dataAsset.getDescription()))
				.addString(checkNull(dataAsset.getPublisher()))
				.addInteger(dataAsset.getStatus() == null ? DataAssetStatus.UNAPPROVED.ordinal() : dataAsset.getStatus().ordinal())
				.addStringArray(dataAsset.getTags() == null ||dataAsset.getTags().isEmpty() ? new String[0] : dataAsset.getTags().toArray(new String[0]))
				.addString(checkNull(dataAsset.getVersion()))
				.addLong(dataAsset.getSourceId())
				.addString(checkNull(dataAssetAdditionalData.getJsonArray("pid").getString(0)))
				.addString(checkNull(dataAssetAdditionalData.getJsonArray("author").getString(0)))
				.addString(checkNull(dataAssetAdditionalData.getJsonArray("data_access_level").getString(0)));

				datasetParams.addValue(processAdditionalMetadata(dataAsset));

		databaseConnector.query(INSERT_DATASET, datasetParams, datasetReply -> {
			if (datasetReply.failed()) {
				LOGGER.error(datasetReply.cause());
			} else {
				for(Distribution distribution : dataAsset.getDistributions()){
					JsonObject distributionAdditionalData = processAdditionalMetadata(distribution);

					Tuple distributionParams = Tuple.tuple()
							.addString(checkNull(distribution.getResourceId()))
							.addString(checkNull(distribution.getLicense()))
							.addString(checkNull(distribution.getTitle()))
							.addString(checkNull(distribution.getDescription()))
							.addString(checkNull(distribution.getPublisher()))
							.addString(checkNull(distribution.getFilename()))
							.addString(checkNull(distribution.getFiletype()))
							.addInteger(Integer.parseInt(checkNull(distributionAdditionalData.getJsonArray("byte_size").getString(0))))
							.addString(checkNull(dataAsset.getResourceId()));

					distributionParams.addValue(processAdditionalMetadata(distribution));

					databaseConnector.query(INSERT_DISTRIBUTION, distributionParams, distributionReply -> {
						if (distributionReply.failed()) {
							LOGGER.error(distributionReply.cause());
						}
					});
				}
			}
		});
		resultHandler.handle(Future.succeededFuture());
	}

	JsonObject processAdditionalMetadata(Resource resource){
		JsonObject jsonObj = new JsonObject();
		if(resource.getAdditionalmetadata() != null) {
			Iterator disIt = resource.getAdditionalmetadata().entrySet().iterator();
			while (disIt.hasNext()) {
				Map.Entry<String, Set<String>> pair = (Map.Entry) disIt.next();
				JsonArray array = new JsonArray();
				for (String metadata : pair.getValue()) {
					array.add(metadata);
				}
				jsonObj.put(pair.getKey(), array);
				disIt.remove();
				if(pair.getKey().equals("pid") || pair.getKey().equals("author") || pair.getKey().equals("data_access_level") || pair.getKey().equals("byte_size"))
					resource.getAdditionalmetadata().remove(pair.getKey(), pair.getValue());
			}
		}
		return jsonObj;
	}

	public void delete(Long id, Handler<AsyncResult<Void>> resultHandler) {

		findDatasetById(id, daReply -> {
			if(daReply.succeeded()){
				databaseConnector.query(DELETE_DIST_UPDATE, Tuple.tuple().addString(daReply.result().getString("resourceid")), distReply -> {});
				databaseConnector.query(DELETE_DAT_UPDATE, Tuple.tuple().addLong(id), datReply -> {});
				resultHandler.handle(Future.succeededFuture());

			} else {
				LOGGER.error(daReply.cause());
				resultHandler.handle(Future.failedFuture(daReply.cause()));
			}
		});
	}
}
