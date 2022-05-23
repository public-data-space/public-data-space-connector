package de.fraunhofer.fokus.ids.controllers;

import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.models.DataAssetDescription;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.Dataset;
import de.fraunhofer.fokus.ids.persistence.entities.serialization.DataSourceSerializer;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import de.fraunhofer.fokus.ids.persistence.enums.JobStatus;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.persistence.managers.JobManager;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.text.ParseException;
import java.util.*;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetController {

    private Logger LOGGER = LoggerFactory.getLogger(DataAssetController.class.getName());
    private DataAssetManager dataAssetManager;
    private DataSourceAdapterService dataSourceAdapterService;
    private DataSourceManager dataSourceManager;
    private JobManager jobManager;
    private BrokerController brokerController;

    public DataAssetController(Vertx vertx) {
        dataAssetManager = new DataAssetManager(vertx);
        jobManager = new JobManager();
        this.dataSourceManager = new DataSourceManager();
        dataSourceAdapterService = DataSourceAdapterService.createProxy(vertx, Constants.DATASOURCEADAPTER_SERVICE);
        brokerController = new BrokerController(vertx);
    }

    public void counts(Handler<AsyncResult<JsonObject>> resultHandler) {
        Promise<Long> count = Promise.promise();
        dataAssetManager.count(reply -> {
            if (reply.succeeded()) {
                count.complete(reply.result());
            } else {
                LOGGER.error("Count could not be queried.\n\n" + reply.cause());
                count.fail(reply.cause());
            }
        });

        Promise<Long> countPublished = Promise.promise();
        dataAssetManager.countPublished(reply -> {
            if (reply.succeeded()) {
                countPublished.complete(reply.result());
            } else {
                LOGGER.error("Published count could not be queried.\n\n" + reply.cause());
                countPublished.fail(reply.cause());
            }
        });

        CompositeFuture.all(count.future(), countPublished.future()).onComplete(ar -> {
            if (ar.succeeded()) {
                JsonObject jO = new JsonObject();
                jO.put("dacount", count.future().result());
                jO.put("publishedcount", countPublished.future().result());
                resultHandler.handle(Future.succeededFuture(jO));
            } else {
                LOGGER.error("Composite Future failed.",  ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void add(DataAssetDescription dataAssetDescription, String licenceurl, String licencetitle, Handler<AsyncResult<JsonObject>> resultHandler) {
        if (dataAssetDescription.getData().isEmpty()) {
            JsonObject jO = new JsonObject();
            jO.put("status", "error");
            jO.put("text", "Bitte geben Sie eine Resource-ID ein!");
            resultHandler.handle(Future.succeededFuture(jO));
        } else {
            jobManager.add(dataAssetDescription, jobReply -> {

                if (jobReply.succeeded()) {
                    long jobId = jobReply.result().getLong("id");
                    LOGGER.info("Starting Job with ID: " + jobId);
                    jobManager.updateStatus(jobId, JobStatus.RUNNING, statusUpdateReply -> {
                    });
                    initiateDataAssetCreation(da -> createDataAsset(jobId, da, licenceurl, licencetitle), dataAssetDescription);
                    JsonObject jO = new JsonObject();
                    jO.put("status", "success");
                    jO.put("text", "Job wurde erstellt!");
                    resultHandler.handle(Future.succeededFuture(jO));
                } else {
                    LOGGER.error("Der Job konnte nicht erstellt werden!", jobReply.cause());
                    JsonObject jO = new JsonObject();
                    jO.put("status", "error");
                    jO.put("text", "Der Job konnte nicht erstellt werden!");
                    resultHandler.handle(Future.succeededFuture(jO));
                }
            });
        }
    }

    private void initiateDataAssetCreation(Handler<AsyncResult<Dataset>> next, DataAssetDescription dataAssetDescription) {
        dataSourceManager.findById(Integer.toUnsignedLong(dataAssetDescription.getSourceId()), dataSourceReply -> {
            if (dataSourceReply.succeeded()) {
                DataSource dataSource;
                try {
                    dataSource = DataSourceSerializer.deserialize(dataSourceReply.result());
                } catch (ParseException e) {
                    LOGGER.error(e);
                    next.handle(Future.failedFuture(e));
                    return;
                }

                DataAssetCreateMessage mes = new DataAssetCreateMessage();
                mes.setData(new JsonObject(dataAssetDescription.getData()));
                mes.setFiles(dataAssetDescription.getFiles());
                mes.setDataSource(dataSource);
                final long datasourceId = dataSource.getId();
                dataSourceAdapterService.createDataAsset(dataSource.getDatasourceType(), new JsonObject(Json.encode(mes)), dataAssetCreateReply -> {
                    if (dataAssetCreateReply.succeeded()) {
                        if (dataAssetCreateReply.result() == null) {
                            LOGGER.error("No DataAsset created.");
                            next.handle(Future.failedFuture("No DataAsset created."));
                        } else {
                            Dataset dataset = Json.decodeValue(dataAssetCreateReply.result().toString(), Dataset.class);
                            dataset.setSourceId(datasourceId);
                            next.handle(Future.succeededFuture(dataset));
                        }
                    } else {
                        LOGGER.error(dataAssetCreateReply.cause());
                        next.handle(Future.failedFuture(dataAssetCreateReply.cause()));
                    }
                });
            } else {
                LOGGER.error(dataSourceReply.cause());
                next.handle(Future.failedFuture(dataSourceReply.cause()));
            }
        });
    }

	private void createDataAsset(long jobId, AsyncResult<Dataset> res, String licenceurl, String licencetitle) {
		if (res.succeeded()) {
		    Dataset dataAsset = res.result();
		    if(dataAsset.getLicense() == null){
		        dataAsset.setLicense(licenceurl);
            }
			LOGGER.info("DataAsset was successfully created.");
			dataAssetManager.add(new JsonObject(Json.encode(dataAsset)), reply -> {
				if (reply.succeeded()) {
					LOGGER.info("DataAsset was successfully inserted to the DB.");
					jobManager.updateStatus(jobId, JobStatus.FINISHED, reply2 -> {});
				} else {
					LOGGER.error("DataAsset insertion failed.", res.cause());
					jobManager.updateStatus(jobId, JobStatus.ERROR, reply2 -> {});
				}
			});
		} else {
			LOGGER.error("DataAsset Creation failed.", res.cause());
			jobManager.updateStatus(jobId, JobStatus.ERROR, reply2 -> {});
		}
	}


	public void publishAll(Handler<AsyncResult<JsonObject>> resultHandler) {
        dataAssetManager.findAll(reply -> {
            if (reply.succeeded()) {
                ArrayList<Future> publishFutures = new ArrayList<>();
                for (int i = 0; i < reply.result().size(); i++) {
                    Dataset da = Json.decodeValue(reply.result().getJsonObject(i).toString(), Dataset.class);
                    Promise promise = Promise.promise();
                    dataAssetManager.changeStatus(DataAssetStatus.PUBLISHED, da.getId(), promise.future());
                }
                CompositeFuture.all(publishFutures).onComplete(reply2 -> {
                    if (reply2.succeeded()) {
                        brokerController.update(reply3 -> {
                            if (reply3.succeeded()) {
                                JsonObject jO = new JsonObject();
                                jO.put("success", "Data Assets wurden veröffentlicht.");
                                resultHandler.handle(Future.succeededFuture(jO));
                            } else {
                                LOGGER.error(reply3.cause());
                                resultHandler.handle(Future.failedFuture(reply3.cause()));
                            }
                        });
                    } else {
                        LOGGER.error(reply2.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void unpublishAll(Handler<AsyncResult<JsonObject>> resultHandler) {
        dataAssetManager.findAll(reply -> {
            if (reply.succeeded()) {
                ArrayList<Future> publishFutures = new ArrayList<>();
                for (int i = 0; i < reply.result().size(); i++) {
                    Dataset da = Json.decodeValue(reply.result().getJsonObject(i).toString(), Dataset.class);
                    Promise promise = Promise.promise();
                    dataAssetManager.changeStatus(DataAssetStatus.APPROVED, da.getId(), promise.future());
                }
                CompositeFuture.all(publishFutures).onComplete(reply2 -> {
                    if (reply2.succeeded()) {
                        brokerController.update(reply3 -> {
                            if (reply3.succeeded()) {
                                JsonObject jO = new JsonObject();
                                jO.put("success", "Data Assets wurden zurückgehalten.");
                                resultHandler.handle(Future.succeededFuture(jO));
                            } else {
                                LOGGER.error(reply3.cause());
                                resultHandler.handle(Future.failedFuture(reply3.cause()));
                            }
                        });
                    } else {
                        LOGGER.error(reply2.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

	public void publish(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataAssetManager.changeStatus(DataAssetStatus.PUBLISHED, id, reply -> {
			JsonObject jO = new JsonObject();
			if (reply.succeeded()) {
				brokerController.update(reply2 -> {
				    if(reply2.succeeded()){
                        jO.put("success", "Data Asset " + id + " wurde veröffentlicht.");
                        resultHandler.handle(Future.succeededFuture(jO));
                    } else {
                        LOGGER.error(reply.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
			}
			else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	public void unPublish(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataAssetManager.changeStatus(DataAssetStatus.APPROVED, id, reply -> {
			JsonObject jO = new JsonObject();
			if (reply.succeeded()) {
                brokerController.update(reply2 -> {
                    if(reply2.succeeded()){
                        jO.put("success", "Data Asset " + id + " wurde zurückgehalten.");
                        resultHandler.handle(Future.succeededFuture(jO));
                    } else {
                        LOGGER.error(reply.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
			}
			else {
				LOGGER.error(reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));


			}
		});
	}

	public void delete(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dataAssetManager.findDatasetById(id, dataAssetReply -> {
			if(dataAssetReply.succeeded()){
			    Dataset ds = Json.decodeValue(dataAssetReply.result().toString(), Dataset.class);
				dataSourceManager.findById(ds.getSourceId(), reply2 -> {
				    if(reply2.succeeded()){
                        Promise<JsonObject> serviceDeletePromise = Promise.promise();
						dataSourceAdapterService.delete(reply2.result().getString("datasourcetype"), ds.getResourceId(), serviceDeletePromise.future());

						Promise<Void> databaseDeletePromise = Promise.promise();
						dataAssetManager.delete(id, databaseDeletePromise.future());

						CompositeFuture.all(databaseDeletePromise.future(), serviceDeletePromise.future()).onComplete( ar -> {
							if(ar.succeeded()){
								brokerController.update(reply -> {
								    if(reply.succeeded()){
                                        JsonObject jO = new JsonObject();
                                        jO.put("status", "success");
                                        jO.put("text", "Data Asset " + id + " wurde gelöscht.");
                                        resultHandler.handle(Future.succeededFuture(jO));
                                    } else {
                                        JsonObject jO = new JsonObject();
                                        jO.put("status", "info");
                                        jO.put("text", "Data Asset " + id + " wurde gelöscht, konnte aber beim Broker nicht entfernt werden.");
                                        resultHandler.handle(Future.succeededFuture(jO));
                                    }
                                });
							} else {
								LOGGER.error("Delete Future could not be completed.", ar.cause());
								resultHandler.handle(Future.failedFuture(ar.cause()));
							}
						});

					} else {
						resultHandler.handle(Future.failedFuture(reply2.cause()));
					}
				});
			} else {
				resultHandler.handle(Future.failedFuture(dataAssetReply.cause()));
			}
		});
	}

	public void index(Handler<AsyncResult<JsonArray>> resultHandler) {
		dataAssetManager.findAll(reply -> {
			if (reply.succeeded()) {
				resultHandler.handle(Future.succeededFuture(reply.result()));

			}
			else {
				LOGGER.error("FindAll Future could not be completed.\n\n" + reply.cause());
				resultHandler.handle(Future.failedFuture(reply.cause()));
			}
		});
	}

	public void resource(Message<Object> receivedMessage) {
		//TODO Get REsource from Adapter
	}

}
