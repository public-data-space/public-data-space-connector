package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.main.ApplicationConfig;
import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.entities.Dataset;
import de.fraunhofer.fokus.ids.persistence.entities.Distribution;
import de.fraunhofer.fokus.ids.persistence.entities.Resource;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import io.netty.channel.unix.Buffer;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.multipart.MultipartForm;
import io.vertx.serviceproxy.ServiceBinder;
import io.vertx.sqlclient.Tuple;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import org.jsoup.Jsoup;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static de.fraunhofer.fokus.ids.persistence.util.Functions.checkNull;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 *
 * @newest_changeses_and_notes_of_Zead:
 *      @properties:
 * 			@INSERT_DATASET: columns oid, author, data_access_level and their values as parameters are added.
 * 			@INSERT_DISTRIBUTION: column byte_size and its value as parameter are added.
 *      @methods: (#some_key is a key of the adjustment that you can search for.)
 * 			@add (edited)
 *      		#StoreDataInDatabaseDataAsset:
 *      			Data that I have received from adapter are saved in object dataset. I extract the additional data from
 *      			this object into JsonObject using the method #processAdditionalMetadata. then we put this data in
 *      			database query as parameters.
 *      		#StoreDataInDatabaseDistirbution:
 *      			I do exactly the same as #StoreDataInDatabaseDataAsset
 *      	@processAdditionalMetadata: (new method)
 *      		Remove the data from Dataset.additionalData and save it in a jsonObject. to be saved in the database.
 *      		If we do not remove these data	from Dataset.additionalData, then they will be saved in additionaldata
 *      		column in database. see #saveAdditionalData
 *      	@findDatasetList: (edited)
 *      		#formatDataAssetDataAsDatasetObject:
 *      			DataAsset Info that called from database should be formatted in Dataset object before I send it to
 *      			client. This task does the method #buildDataAssetAdditionalData.
 *          @buildDataset: (edited)
 *       		#formatDistributionDataAsDistributionObject:
 *      			Distribution Info that called from database should be formatted in Distribution object before I send
 *      			it to client. This task does the method #buildDistributionAdditionalData.
 *      	@buildDataAssetAdditionalData:
 *      		this method remove data asset data from the first level in the given jsonObject	and save it in the same
 *      		object but in additionalData array as <String, String[]>.
 *      	@buildDistributionAdditionalData:
 *      		Does the same thing as buildDataAssetAdditionalData, so I will combine these two methods in one.
 */
public class DataAssetManager {

	private Logger LOGGER = LoggerFactory.getLogger(DataAssetManager.class.getName());
	private DatabaseConnector databaseConnector;
	private Vertx vertx;
	WebClient webClient;

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
	
	private static final String UPDATE_TAGS = "UPDATE dataset SET tags = $1, updated_at = NOW() WHERE resourceid = $2";

	public DataAssetManager(Vertx vertx) {
		this.databaseConnector = DatabaseConnector.getInstance();
		this.vertx = vertx;
		this.webClient = WebClient.create(vertx);
	}

	public void findDatasetById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		querySingleEntry(id, FINDBYDATASETID_QUERY, resultHandler);
	}

	public void findDatasetByResourceId(String resourceId, Handler<AsyncResult<JsonObject>> resultHandler) {
		querySingleEntry(resourceId, FINDBYDATASETRESOURCEID_QUERY, resultHandler);
	}

	public void findDistributionById(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		querySingleEntry(id, FINDBYDISTRIBUTIONID_QUERY, resultHandler);
	}

	private void querySingleEntry(Object id, String query, Handler<AsyncResult<JsonObject>> resultHandler) {
		databaseConnector.query(query, Tuple.tuple().addValue(id), reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			} else {
				if (reply.result().isEmpty()) {
					resultHandler.handle(Future.failedFuture("Id not in database"));
				} else {
					resultHandler.handle(Future.succeededFuture(reply.result().get(0)));
				}
			}
		});
	}

	private void findDatasetList(String query, Tuple tuple, Handler<AsyncResult<JsonArray>> resultHandler) {
		databaseConnector.query(query, tuple, reply -> {
			if (reply.failed()) {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause().toString()));
			} else {
				JsonArray jA = new JsonArray(reply.result());
				Iterator iterator = jA.iterator();
				List<Future> datasetFutureList = new ArrayList<>();
				while (iterator.hasNext()) {
					Promise<Dataset> promise = Promise.promise();
					datasetFutureList.add(promise.future());
					//#formatDataAssetDataAsDatasetObject
					Dataset da = Json.decodeValue(this.buildDataAssetAdditionalData((JsonObject)iterator.next()), Dataset.class);
					buildDataset(da, promise);
				}
				CompositeFuture.all(datasetFutureList).onComplete(ac -> {
					if (ac.succeeded()) {
						List<Dataset> datasetList = new ArrayList<>();
						for (Future fu : datasetFutureList) {
							datasetList.add((Dataset) fu.result());
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
		findDatasetList(FINDPUBLISHED_QUERY, Tuple.tuple().addInteger(DataAssetStatus.PUBLISHED.ordinal()),
				resultHandler);
	}

	private void buildDataset(Dataset da, Handler<AsyncResult<Dataset>> next) {
		databaseConnector.query(FINDDISTRIBUTIONBYDATASETID_QUERY, Tuple.tuple().addString(da.getResourceId()), reply2 -> {
			if(reply2.succeeded()){
				//#formatDistributionDataAsDistributionObject
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
		databaseConnector.query(COUNTPUBLISHED_QUERY, Tuple.tuple().addInteger(DataAssetStatus.PUBLISHED.ordinal()),
				reply -> {
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
		//#StoreDataInDatabaseDataAsset
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

		//#saveAdditionalData
		datasetParams.addValue(processAdditionalMetadata(dataAsset));

		databaseConnector.query(INSERT_DATASET, datasetParams, datasetReply -> {
			if (datasetReply.failed()) {
				LOGGER.error(datasetReply.cause());
			} else {
				for(Distribution distribution : dataAsset.getDistributions()){
					//#StoreDataInDatabaseDistirbution
					JsonObject distributionAdditionalData = processAdditionalMetadata(distribution);

					Tuple distributionParams = Tuple.tuple().addString(checkNull(distribution.getResourceId()))
							.addString(checkNull(distribution.getLicense()))
							.addString(checkNull(distribution.getTitle()))
							.addString(checkNull(distribution.getDescription()))
							.addString(checkNull(distribution.getPublisher()))
							.addString(checkNull(distribution.getFilename()))
							.addString(checkNull(distribution.getFiletype()))
							.addInteger(Integer.parseInt(checkNull(distributionAdditionalData.getJsonArray("byte_size").getString(0))))
							.addString(checkNull(dataAsset.getResourceId()));

					//#saveAdditionalData
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
	

	public void updateTagsFromDescription(Dataset dataAsset, Handler<AsyncResult<JsonObject>> resultHandler) {

		String description = checkNull(dataAsset.getDescription());
		description = Jsoup.parse(description).text();
		MultiMap form = MultiMap.caseInsensitiveMultiMap();
		form.add("text", description);
		
		ConfigRetriever retriever = ConfigRetriever.create(vertx);
	       retriever.getConfig(ar2 -> {
	           if (ar2.succeeded()) {
	               JsonObject env = ar2.result();
	               String annif = env.getString(ApplicationConfig.ENV_ANNIF, ApplicationConfig.DEFAULT_ANNIF);
	               //webClient.postAbs("https://annif.apps.osc.fokus.fraunhofer.de/v1/projects/data-theme-nn-ensemble-en/suggest")
	               boolean isHTTPS = false;
	               if (annif.contains("https")) {
	            	   isHTTPS = true;
	               }
	               webClient.postAbs(annif)
	               		.ssl(isHTTPS).putHeader("content-type", "multipart/form-data").sendForm(form, ar -> {
							if (ar.succeeded()) {
		
								HttpResponse<io.vertx.core.buffer.Buffer> response = ar.result();
								LOGGER.info(response.toString());
								JsonObject json = response.bodyAsJsonObject();
								JsonArray jsonArray = (JsonArray) json.getValue("results");
								Set<String> tags = dataAsset.getTags();
								int tagsSize = 0;
								if (tags != null) {
									tagsSize = tags.size();
								}
								String[] result = new String[jsonArray.size() + tagsSize];
								for (int i = 0; i < jsonArray.size(); i++) {
									result[i] = jsonArray.getJsonObject(i).getString("label");
								}
		
								int j = 0;
								if (tagsSize != 0) {
									for (String tag : tags) {
										result[jsonArray.size() + j] = tag;
										j++;
									}
								}
		
								Tuple params = Tuple.tuple().addStringArray(result).addString(dataAsset.getResourceId());
		
								databaseConnector.query(UPDATE_TAGS, params, reply -> {
									if (reply.failed()) {
										LOGGER.error(reply.cause());
									} else {
										JsonObject jO = new JsonObject();
										String [] newTags = params.getStringArray(0);
										String tagsResult = "";
										for (int i = 0; i<newTags.length; i++) {
											tagsResult += newTags[i] + ", ";
										}
										if(tagsResult.length() > 3) {
											tagsResult = tagsResult.substring(0, tagsResult.length()-2);
										}
										
										jO.put("tags", tagsResult);
										resultHandler.handle(Future.succeededFuture(jO));	
									}
								});
		
							} else {
								LOGGER.info(ar.cause());
							}
						});   
	           } else {
	               LOGGER.error("Config could not be retrieved.");
	           }
	       });
	}

	JsonObject processAdditionalMetadata(Resource resource) {
		JsonObject jsonObj = new JsonObject();
		if (resource.getAdditionalmetadata() != null) {
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
			if (daReply.succeeded()) {
				databaseConnector.query(DELETE_DIST_UPDATE,
						Tuple.tuple().addString(daReply.result().getString("resourceid")), distReply -> {
						});
				databaseConnector.query(DELETE_DAT_UPDATE, Tuple.tuple().addLong(id), datReply -> {
				});
				resultHandler.handle(Future.succeededFuture());

			} else {
				LOGGER.error(daReply.cause());
				resultHandler.handle(Future.failedFuture(daReply.cause()));
			}
		});
	}
}
