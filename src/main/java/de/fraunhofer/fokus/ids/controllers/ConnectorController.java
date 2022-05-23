package de.fraunhofer.fokus.ids.controllers;

import com.fasterxml.jackson.annotation.JsonInclude;
import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.models.*;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.Dataset;
import de.fraunhofer.fokus.ids.persistence.entities.Distribution;
import de.fraunhofer.fokus.ids.persistence.entities.serialization.DataSourceSerializer;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.persistence.managers.DataSourceManager;
import de.fraunhofer.fokus.ids.services.ConfigService;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterService;
import de.fraunhofer.fokus.ids.services.datasourceAdapter.DataSourceAdapterDownloadService;
import de.fraunhofer.fokus.ids.utils.models.IDSMessage;
import de.fraunhofer.fokus.ids.utils.services.authService.AuthAdapterService;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Optional;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class ConnectorController {

	private Logger LOGGER = LoggerFactory.getLogger(ConnectorController.class.getName());
	private IDSService idsService;
	private DataAssetManager dataAssetManager;
	private DataSourceManager dataSourceManager;
	private DataSourceAdapterService dataSourceAdapterService;
    private AuthAdapterService authAdapterService;
    private ConfigService configService;
	private Serializer serializer;
	private DataSourceAdapterDownloadService dataSourceAdapterDownloadService;

	public ConnectorController(Vertx vertx){
		this.idsService = new IDSService(vertx);
		this.authAdapterService = AuthAdapterService.createProxy(vertx, Constants.AUTHADAPTER_SERVICE);
		this.dataAssetManager = new DataAssetManager(vertx);
		this.dataSourceManager = new DataSourceManager();
		this.dataSourceAdapterService = DataSourceAdapterService.createProxy(vertx, Constants.DATASOURCEADAPTER_SERVICE);
		this.configService = new ConfigService(vertx);
		this.serializer = new Serializer();
		this.dataSourceAdapterDownloadService = new DataSourceAdapterDownloadService(vertx);
		DatabindCodec.prettyMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
	}

	public void data(Message header, String extension, HttpServerResponse httpServerResponse){
		URI uri = ((ArtifactRequestMessage) header).getRequestedArtifact();
		String path = uri.getPath();
		String idStr = path.substring(path.lastIndexOf('/') + 1);
		long id = Long.parseLong(idStr);

		Promise<Message> artifactResponsePromise = Promise.promise();
		Future<Message> artifactResponseFuture = artifactResponsePromise.future();
		configService.getConfiguration(reply -> {
			if(reply.succeeded()) {
				idsService.getArtifactResponse(reply.result(),header.getId(), artifactResponseFuture);
			} else {
				LOGGER.info(reply.cause());
				artifactResponsePromise.fail(reply.cause());
			}
		});
		Promise<File> filePromise = Promise.promise();
		Future<File> fileFuture = filePromise.future();
		payload(false,id, extension, fileFuture, null);
		idsService.handleDataMessage(header.getId(), artifactResponseFuture, fileFuture, id, httpEntityAsyncResult -> replyMessage(httpEntityAsyncResult,httpServerResponse));
	}

	public void payload(boolean download,long id, String extension, Handler<AsyncResult<File>> resultHandler, HttpServerResponse httpServerResponse){
		if (!download){
			buildDataAssetReturn(false, id, extension, reply -> {
				if(reply.succeeded()){
					resultHandler.handle(Future.succeededFuture(reply.result()));
				}
				else{
					resultHandler.handle(Future.failedFuture(reply.cause()));
				}
			},httpServerResponse);
		}else {
			buildDataAssetReturn(true, id, extension, reply -> {
				if (reply.failed()){
					replyMessage(Future.failedFuture(reply.cause()),httpServerResponse);
				}
			},httpServerResponse);
		}
	}

	private void buildDataAssetReturn(boolean download, long id, String extension, Handler<AsyncResult<File>> resultHandler, HttpServerResponse httpServerResponse){
		if(extension == null) {
			payload(download, id, resultHandler, httpServerResponse);
		}
		else {
			payloadContent(download, id,extension, resultHandler, httpServerResponse);
		}
	}

	public void checkMessage(Optional<IDSMessage> input, Class clazz, HttpServerResponse httpServerResponse) {
		if (input.isPresent() && input.get().getHeader().isPresent()) {
			Message header = input.get().getHeader().get();
			if (clazz.isInstance(header)) {
				routeMessage(input, httpServerResponse);
			} else {
				LOGGER.error("Malformed message");
				idsService.handleRejectionMessage(header.getId(), RejectionReason.MALFORMED_MESSAGE,
						httpEntityAsyncResult -> replyMessage(httpEntityAsyncResult,httpServerResponse));
			}
		}
	}

	public void routeMessage(Optional<IDSMessage> input, HttpServerResponse httpServerResponse) {
		if (input.isPresent() && input.get().getHeader().isPresent()) {
			Message header = input.get().getHeader().get();
			String token = "abc123";
			authAdapterService.isAuthenticated(header.getSecurityToken() == null?token:header.getSecurityToken().getTokenValue(), authreply -> {
				if (authreply.succeeded()) {
					if (header instanceof DescriptionRequestMessage) {
						multiPartAbout(header, httpEntityAsyncResult -> replyMessage(httpEntityAsyncResult,httpServerResponse));
					} else if (header instanceof ArtifactRequestMessage) {
						data(header, "", httpServerResponse);
					} else {
						LOGGER.error("Messagetype not supported.");
						idsService.handleRejectionMessage(header.getId(), RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED, httpEntityAsyncResult -> replyMessage(httpEntityAsyncResult,httpServerResponse));
					}
				} else {
					LOGGER.error("Not authenticated.");
					idsService.handleRejectionMessage(header.getId(), RejectionReason.NOT_AUTHENTICATED, httpEntityAsyncResult -> replyMessage(httpEntityAsyncResult,httpServerResponse));
				}
			});
		} else {
			URI uri = null;
			try {
				uri = new URI("http://example.org");
			} catch (URISyntaxException e) {
				LOGGER.error("URI coulod not be created");
				replyMessage(Future.failedFuture(e),httpServerResponse);
			}
			LOGGER.error("Malformed message");
			idsService.handleRejectionMessage(uri, RejectionReason.MALFORMED_MESSAGE, httpEntityAsyncResult -> replyMessage(httpEntityAsyncResult,httpServerResponse));
		}
	}

	public void multiPartAbout(Message header, Handler<AsyncResult<HttpEntity>> resultHandler) {
		Promise<Connector> connectorPromise = Promise.promise();
		Future<Connector> connectorFuture = connectorPromise.future();
		Promise<Message> responsePromise = Promise.promise();
		Future<Message> responseFuture = responsePromise.future();

		configService.getConfiguration(reply -> {
			if(reply.succeeded()) {
				idsService.getConnector(reply.result(), connectorFuture);
				idsService.getSelfDescriptionResponse(reply.result(), header.getId(), responseFuture);
			} else {
				LOGGER.info(reply.cause());
				connectorPromise.fail(reply.cause());
				responsePromise.fail(reply.cause());
			}
		});
		idsService.handleAboutMessage(header.getId(), responseFuture, connectorFuture, resultHandler);
	}

	public void about(Handler<AsyncResult<String>> resultHandler) {
		configService.getConfiguration(configReply -> {
			if(configReply.succeeded()) {
				idsService.getConnector(configReply.result(), reply -> {
					if (reply.succeeded()) {
						try {
							resultHandler.handle(Future.succeededFuture(serializer.serialize(reply.result())));
						} catch (IOException e) {
							LOGGER.error(e);
							resultHandler.handle(Future.failedFuture(e));
						}
					} else {
						LOGGER.error("Connector Object could not be retrieved.", reply.cause());
						resultHandler.handle(Future.failedFuture(reply.cause()));
					}
				});
			} else {
				LOGGER.error(configReply.cause());
				resultHandler.handle(Future.failedFuture(configReply.cause()));
			}
		});
	}

	private void payload(boolean download, Long id, Handler<AsyncResult<File>> resultHandler, HttpServerResponse httpServerResponse) {
		getPayload(download, id, FileType.MULTIPART, resultHandler, httpServerResponse);
	}

	private void payloadContent(boolean download, Long id, String extension, Handler<AsyncResult<File>> resultHandler, HttpServerResponse httpServerResponse) {
		if(extension.equals("json")) {
			getPayload(download, id, FileType.JSON, resultHandler, httpServerResponse);
		}
		if(extension.equals("txt")) {
			getPayload(download , id, FileType.TXT, resultHandler, httpServerResponse);
		}
		getPayload(download, id, FileType.MULTIPART, resultHandler, httpServerResponse);
	}

	private void getPayload(boolean download, Long id, FileType fileType, Handler<AsyncResult<File>> resultHandler, HttpServerResponse httpServerResponse) {
		dataAssetManager.findDistributionById(id, distReply -> {
			if (distReply.succeeded()) {
				Distribution distribution = Json.decodeValue(distReply.result().toString(), Distribution.class);
				dataAssetManager.findDatasetByResourceId(distribution.getDatasetId(), datReply -> {
					if (datReply.succeeded()) {
						Dataset dataset = Json.decodeValue(datReply.result().toString(), Dataset.class);
						dataSourceManager.findById(dataset.getSourceId(), reply2 -> {
							if (reply2.succeeded()) {
								DataSource dataSource = null;
								try {
									dataSource = DataSourceSerializer.deserialize(reply2.result());
								} catch (ParseException e) {
									LOGGER.error(e);
									resultHandler.handle(Future.failedFuture(e));
								}

								ResourceRequest request = new ResourceRequest();
								request.setDataSource(dataSource);
								request.setDataAsset(distribution);
								request.setFileType(fileType);
								if (download){
									dataSourceAdapterDownloadService.getFile(dataSource.getDatasourceType(),
											new JsonObject(Json.encode(request)),httpServerResponse);
								}else {
									dataSourceAdapterService.getFile(dataSource.getDatasourceType(), new JsonObject(Json.encode(request)), reply3 -> {
										if (reply3.succeeded()) {
											resultHandler.handle(Future.succeededFuture(new File(reply3.result())));
										} else {
											LOGGER.error("FileContent could not be retrieved.", reply3.cause());
											resultHandler.handle(Future.failedFuture(reply3.cause()));
										}
									});
								}

							} else {
								LOGGER.error("DataAsset could not be retrieved.", reply2.cause());
								resultHandler.handle(Future.failedFuture(reply2.cause()));
							}
						});
					}
				});
			} else {
				LOGGER.error(distReply.cause());
				resultHandler.handle(Future.failedFuture(distReply.cause()));
			}
		});
	}

	private void replyMessage(AsyncResult<HttpEntity> result, HttpServerResponse response){
		if(result.succeeded()){
			if(result.result() != null) {
				try(ByteArrayOutputStream baos = new ByteArrayOutputStream()){
					Header contentTypeHeader =  result.result().getContentType();
					result.result().writeTo(baos);
					response.putHeader(contentTypeHeader.getName(), contentTypeHeader.getValue());
					response.end(Buffer.buffer(baos.toByteArray()));
				} catch (IOException e) {
					LOGGER.error(e);
					response.setStatusCode(500);
					response.end();
				}
			}
		}
		else{
			LOGGER.error("Result Future failed.",result.cause());
			response.setStatusCode(500).end();
		}
	}
}