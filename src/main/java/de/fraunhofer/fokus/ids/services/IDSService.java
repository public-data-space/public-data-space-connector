package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.models.Constants;
import de.fraunhofer.fokus.ids.persistence.entities.Dataset;
import de.fraunhofer.fokus.ids.persistence.entities.Distribution;
import de.fraunhofer.fokus.ids.persistence.managers.DataAssetManager;
import de.fraunhofer.fokus.ids.utils.services.authService.AuthAdapterService;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import de.fraunhofer.iais.eis.util.TypedLiteral;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.List;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class IDSService {
	private final Logger LOGGER = LoggerFactory.getLogger(IDSService.class.getName());

	private String INFO_MODEL_VERSION = "4.0.0";
	private String[] SUPPORTED_INFO_MODEL_VERSIONS = {"4.0.0"};
	private static final String CONNECTOR_VERSION = "2.0.0";
	private DataAssetManager dataAssetManager;
	private ConfigService configService;
    private Serializer serializer = new Serializer();
	private AuthAdapterService authAdapterService;

	public IDSService(Vertx vertx){
		dataAssetManager = new DataAssetManager(vertx);
		configService = new ConfigService(vertx);
		this.authAdapterService = AuthAdapterService.createProxy(vertx, Constants.AUTHADAPTER_SERVICE);

	}

	public void getSelfDescriptionResponse(JsonObject config, URI uri, Handler<AsyncResult<Message>> resultHandler) {
		buildSelfDescriptionResponse(uri, config, resultHandler);
	}

	public void getArtifactResponse(JsonObject config, URI uri, Handler<AsyncResult<Message>> resultHandler) {
		buildArtifactResponseMessage(uri, config, resultHandler);
	}

	private void buildArtifactResponseMessage(URI uri, JsonObject config, Handler<AsyncResult<Message>> resultHandler) {
		getJWT(jwtResult -> {
			if(jwtResult.succeeded()) {
				try {
					ArtifactResponseMessage message = new ArtifactResponseMessageBuilder(new URI(config.getString("url") + "/ArtifactResponseMessage/" + UUID.randomUUID().toString()))
							._issued_(getDate())
							._correlationMessage_(uri)
							._modelVersion_(INFO_MODEL_VERSION)
							._issuerConnector_(new URI(config.getString("url") + "#Connector"))
							._securityToken_(new DynamicAttributeTokenBuilder()
									._tokenFormat_(TokenFormat.JWT)
									._tokenValue_(jwtResult.result())
									.build())
							.build();
					resultHandler.handle(Future.succeededFuture(message));

				} catch (URISyntaxException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
			}
			else {
				LOGGER.error(jwtResult.cause());
				resultHandler.handle(Future.failedFuture(jwtResult.cause()));
			}
		});
	}

	private XMLGregorianCalendar getDate(){
		GregorianCalendar c = new GregorianCalendar();
		c.setTime(new Date());
		try {
			return DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
		} catch (DatatypeConfigurationException e) {
			LOGGER.error(e);
		}
		return null;
	}

	public void createRegistrationMessage(JsonObject config, Handler<AsyncResult<Message>> resultHandler){
		createUpdateMessage(config, resultHandler);
	}

	public void createUpdateMessage(JsonObject config, Handler<AsyncResult<Message>> resultHandler){
		getJWT(jwtResult -> {
			if(jwtResult.succeeded()) {
				try {
					ConnectorUpdateMessage message = new ConnectorUpdateMessageBuilder()
							._issued_(getDate())
							._modelVersion_(INFO_MODEL_VERSION)
							._issuerConnector_(new URI(config.getString("url") + "#Connector"))
							._securityToken_(new DynamicAttributeTokenBuilder()
									._tokenFormat_(TokenFormat.JWT)
									._tokenValue_(jwtResult.result())
									.build())
							.build();
					resultHandler.handle(Future.succeededFuture(message));
				} catch (URISyntaxException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
			} else {
				LOGGER.error(jwtResult.cause());
				resultHandler.handle(Future.failedFuture(jwtResult.cause()));
			}
		});
	}

	public void createUnregistrationMessage(JsonObject config, Handler<AsyncResult<Message>> resultHandler){
		getJWT(jwtResult -> {
			if(jwtResult.succeeded()) {
				try {
					ConnectorUnavailableMessage message = new ConnectorUnavailableMessageBuilder()
							._issued_(getDate())
							._modelVersion_(INFO_MODEL_VERSION)
							._issuerConnector_(new URI(config.getString("url")+"#Connector"))
							._securityToken_( new DynamicAttributeTokenBuilder()
									._tokenFormat_(TokenFormat.JWT)
									._tokenValue_(jwtResult.result())
									.build())
							.build();
					resultHandler.handle(Future.succeededFuture(message));
				} catch (URISyntaxException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
			} else {
				LOGGER.error(jwtResult.cause());
				resultHandler.handle(Future.failedFuture(jwtResult.cause()));
			}
		});
	}

	public void getConnector(JsonObject config, Handler<AsyncResult<Connector>> resultHandler) {
				buildBaseConnector(config, reply -> {
					if (reply.succeeded()) {
						resultHandler.handle(Future.succeededFuture(reply.result()));
					} else {
						LOGGER.error(reply.cause());
						resultHandler.handle(Future.failedFuture(reply.cause()));
					}
				});
	}

	private DescriptionResponseMessage buildSelfDescriptionResponse(URI uri, JsonObject config, Handler<AsyncResult<Message>> resultHandler){
		getJWT(jwtResult -> {
			if(jwtResult.succeeded()) {
				try {
					DescriptionResponseMessage message = new DescriptionResponseMessageBuilder(new URI(config.getString("url") + "/DescriptionResponseMessage/" + UUID.randomUUID()))
							._issued_(getDate())
							._correlationMessage_(uri)
							._modelVersion_(INFO_MODEL_VERSION)
							._issuerConnector_(new URI(config.getString("url") + "#Connector"))
							._securityToken_(new DynamicAttributeTokenBuilder()
									._tokenFormat_(TokenFormat.JWT)
									._tokenValue_(jwtResult.result())
									.build())
							.build();
					resultHandler.handle(Future.succeededFuture(message));
				} catch (URISyntaxException e) {
					LOGGER.error(e);
					resultHandler.handle(Future.failedFuture(e));
				}
			} else {
				LOGGER.error(jwtResult.cause());
				resultHandler.handle(Future.failedFuture(jwtResult.cause()));
			}
		});
		return null;
	}

	private void buildBaseConnector(JsonObject config, Handler<AsyncResult<Connector>> next){

		findPublished(publishedDataAssets -> {
			if(publishedDataAssets.succeeded()){
				HashMap<Long, List<ConnectorEndpoint>> endpointMap = getResourceEndpoints(config, publishedDataAssets.result());
				Future<ResourceCatalog> catalogFuture = buildCatalog(config, publishedDataAssets.result(), endpointMap);
				try {
					BaseConnectorBuilder connectorBuilder = new BaseConnectorBuilder((new URI(config.getString("url") + "#Connector")))
							._maintainer_(new URI(config.getString("maintainer")))
							._version_(CONNECTOR_VERSION)
							._curator_(new URI(config.getString("curator")))
							._physicalLocation_(new GeoFeatureBuilder(new URI(config.getString("country"))).build())
							._outboundModelVersion_(INFO_MODEL_VERSION)
							._inboundModelVersion_(new ArrayList<>(Arrays.asList(SUPPORTED_INFO_MODEL_VERSIONS)))

							//TODO Change dummy auth service (can be null)
							//				._authInfo_(new AuthInfoBuilder(new URL(this.connectorURL + "#AuthInfo"))
							//						._authService_(new URI(this.connectorURL + "#AuthService"))
							//						._authStandard_(AuthStandard.OAUTH2_JWT)
							//						.build())

							._securityProfile_(SecurityProfile.BASE_SECURITY_PROFILE)
							._title_(new ArrayList<>(Arrays.asList(new TypedLiteral(config.getString("title")))))
							._hasEndpoint_(createStaticEndpoints(config));

					//TODO fill with information
					//				._descriptions_(new ArrayList<PlainLiteral>(Arrays.asList(new PlainLiteral(""))))
					//				._lifecycleActivities_(null)
					//				._componentCertification_(null)

					catalogFuture.onComplete( ac -> {
						if(ac.succeeded()) {
							connectorBuilder._resourceCatalog_(new ArrayList(Arrays.asList(catalogFuture.result())));
							next.handle(Future.succeededFuture(connectorBuilder.build()));
						}
						else{
							LOGGER.error(ac.cause());
							next.handle(Future.failedFuture(ac.cause()));
						}
					});

				} catch (Exception e) {
					LOGGER.error(e);
					next.handle(Future.failedFuture(e.getMessage()));
				}

			}
		});
	}

	private Future<ResourceCatalog> buildCatalog(JsonObject config, List<Dataset> publishedDataAssets, Map<Long, List<ConnectorEndpoint>> endpointMap) {

		Future<List<Resource>> offers = getOfferResources(config, publishedDataAssets, endpointMap);
		Future<List<Resource>> requests = getRequestResources(config);
		Promise<ResourceCatalog> catalog = Promise.promise();

		CompositeFuture.all(offers, requests).onComplete(cf -> {
			if(cf.succeeded()) {
				try {
					catalog.complete(new ResourceCatalogBuilder(new URI(config.getString("url") + "#Catalog"))
							._offeredResource_(new ArrayList(offers.result()))
							._requestedResource_(new ArrayList(requests.result()))
							.build());
				} catch (Exception e) {
					LOGGER.error(e);
					catalog.fail(e.getMessage());
				}
			}
			else{
				LOGGER.error(cf.cause());
				catalog.fail(cf.cause());
			}
		});
		return catalog.future();
	}

	private Future<List<Resource>> getRequestResources(JsonObject config) {
		// TODO Auto-generated method stub
		return Future.succeededFuture(new ArrayList<>());
	}

	private Future<List<Resource>> getOfferResources(JsonObject config, List<Dataset> publishedDataAssets, Map<Long, List<ConnectorEndpoint>> endpointMap) {
		Promise<List<Resource>> daPromise = Promise.promise();
		createDataResources(config, publishedDataAssets, daPromise, endpointMap);
		return daPromise.future();
	}

	private void createDataResources( JsonObject config, List<Dataset> daList, Promise<List<Resource>> daPromise, Map<Long, List<ConnectorEndpoint>> endpointMap) {
			ArrayList<Resource> offerResources = new ArrayList<>();
			for (Dataset da : daList) {
				try {
					DataResourceBuilder r = new DataResourceBuilder(new URI(config.getString("url") + "/DataResource/"+da.getId()))
							//						//TODO: The regular period with which items are added to a collection.
							//						._accrualPeriodicity_(null)
							//						//TODO: Reference to a Digital Content (physically or logically) included, definition of part-whole hierarchies.
							//						._contentParts_(null)
							//						//TODO: Constraint that refines a (composite) Digital Content.
							//						._contentRefinements_(null)
							//						//TODO: Standards document defining the given Digital Content. The content is assumed to conform to that Standard.
							//						._contentStandard_(null)
							//						//TODO: Enumerated types of content expanding upon the Digital Content hierarchy.
							//						._contentType_(null)
							//						//TODO: Reference to a Contract Offer defining the authorized use of the Resource.
							//						._contractOffers_(null)
							//						//TODO: Default representation of the content.
							//						._defaultRepresentation_(null)
							//						//TODO: Natural language(s) used within the content
							//						._languages_(null)
							//						//TODO: Something that occurs over a period of time and acts upon or with entities.
							//						._lifecycleActivities_(null)
							//						//TODO: Representation of the content.
							//						._representations_(null)
							//						//TODO: Reference to the Interface defining Operations supported by the Resource.
							//						._resourceInterface_(null)
							//						//TODO: Reference to a Resource (physically or logically) included, definition of part-whole hierarchies.
							//						._resourceParts_(null)
							//						//TODO: Sample Resource instance.
							//						._samples_(null)
							//						//TODO: Named spatial entity covered by the Resource.
							//						._spatialCoverages_(null)
							//						//TODO: Reference to a well-known License regulating the general usage of the Resource.
							//						._standardLicense_(null)
							//						//TODO: Temporal period or instance covered by the content.
							//						._temporalCoverages_(null)
							//						//TODO: Abstract or concrete concept related to or referred by the content.
							//						._themes_(null)
							//						//TODO: (Equivalent) variant of given Resource, e.g. a translation.
							//						._variant_(null)
							//._publisher_(null)
							//._sovereign_(null);
							._version_(da.getVersion())
							._resourceEndpoint_(new ArrayList(endpointMap.get(da.getId())));

					if (da.getTitle() != null) {
						r._title_(new ArrayList<>(Arrays.asList(new TypedLiteral(da.getTitle()))));
					}
					if (da.getDescription() != null && !da.getDescription().isEmpty()) {
						r._description_(new ArrayList<>(Arrays.asList(new TypedLiteral(da.getDescription()))));
					}
					ArrayList<TypedLiteral> keywords = getKeyWords(da);
					if (keywords != null) {
						r._keyword_(keywords);
					}
					if (da.getLicense() != null) {
						r._customLicense_(new URI(da.getLicense()));
					}
					DataResource dr = r.build();
					if(da.getAdditionalmetadata() != null) {
						Iterator it = da.getAdditionalmetadata().entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, Set<String>> pair = (Map.Entry) it.next();
							//Strangely the IDS Infomodell library does not support duplicate JSON keys. Therefore rejoining information
							dr.setProperty(pair.getKey(), String.join(", ", pair.getValue()));
							it.remove();
						}
					}
					offerResources.add(dr);
				} catch (Exception e) {
					LOGGER.error( e);
				}
			}
			daPromise.complete(offerResources);
	}

	private void findPublished(Handler<AsyncResult<List<Dataset>>> next) {

		dataAssetManager.findPublished(reply -> {
			if(reply.succeeded()) {

				JsonArray array = new JsonArray(reply.result().toString());
				List<Dataset> assets = new ArrayList<>();
				for(int i=0;i<array.size();i++){
					assets.add(Json.decodeValue(array.getJsonObject(i).toString(), Dataset.class));
				}
				next.handle(Future.succeededFuture(assets));
			}
			else{
				LOGGER.error(reply.cause());
				next.handle(Future.succeededFuture(new ArrayList<>()));
			}
		});
	}

	private HashMap<Long, List<ConnectorEndpoint>> getResourceEndpoints(JsonObject config, List<Dataset> daList) {
		HashMap<Long, List<ConnectorEndpoint>> endpoints = new HashMap<>();
		for(Dataset da : daList) {
			List<ConnectorEndpoint> daEndpoints = new ArrayList<>();
			for(Distribution dist : da.getDistributions()) {
				try {
					ConnectorEndpoint e = new ConnectorEndpointBuilder(new URI(config.getString("url") + "/ConnectorEndpoint/" + UUID.randomUUID()))
							._endpointArtifact_(new ArtifactBuilder(new URI(config.getString("url") + "/Artifact/" + dist.getId()))
									._creationDate_(getDate(dist.getCreatedAt()))
									._fileName_(dist.getFilename())
									.build())
							._accessURL_(new URI(config.getString("url") + "/data/" + dist.getId().toString()))
							._endpointInformation_(new ArrayList<>(Arrays.asList(new TypedLiteral("The file " + dist.getFilename() + " can be obtained from this endpoint via GET request."))))
							.build();
					e.setProperty(DCTERMS.TITLE.toString(), dist.getTitle());
					e.setProperty(DCTERMS.DESCRIPTION.toString(), dist.getDescription());
					e.setProperty(DCTERMS.FORMAT.toString(), dist.getFiletype());
					e.setProperty(DCTERMS.LICENSE.toString(), dist.getLicense());
					if(dist.getAdditionalmetadata() != null) {
						Iterator it = dist.getAdditionalmetadata().entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, Set<String>> pair = (Map.Entry) it.next();
							//Strangely the IDS Infomodell library does not support duplicate JSON keys. Therefore rejoining information
							e.setProperty(pair.getKey(), String.join(", ", pair.getValue()));
							it.remove();
						}
					}
					daEndpoints.add(e);
				} catch (Exception e1) {
					LOGGER.error(e1);
				}
			}
			endpoints.put(da.getId(), daEndpoints);
		}
		return endpoints;
	}

	private ArrayList<? extends ConnectorEndpoint> createStaticEndpoints(JsonObject config) {
		ArrayList<ConnectorEndpoint> endpoints = new ArrayList<>();
			try {
				ConnectorEndpoint e = new ConnectorEndpointBuilder(new URI(config.getString("url") + "/ConnectorEndpoint/" + UUID.randomUUID()))
						._accessURL_(new URI(config.getString("url") + "/infrastructure"))
						._endpointInformation_(new ArrayList<>(Arrays.asList(new TypedLiteral("All IDS Multipart messages should be sent to this endpoint."))))
						.build();
				endpoints.add(e);
				ConnectorEndpoint e2 = new ConnectorEndpointBuilder(new URI(config.getString("url") + "/ConnectorEndpoint/" + UUID.randomUUID()))
						._accessURL_(new URI(config.getString("url") + "/data"))
						._endpointInformation_(new ArrayList<>(Arrays.asList(new TypedLiteral("IDS ArtifactRequestMessages can be sent to this endpoint via POST request."))))
						.build();
				endpoints.add(e2);
				ConnectorEndpoint e3 = new ConnectorEndpointBuilder(new URI(config.getString("url") + "/ConnectorEndpoint/" + UUID.randomUUID()))
						._accessURL_(new URI(config.getString("url") + "/about"))
						._endpointInformation_(new ArrayList<>(Arrays.asList(new TypedLiteral("IDS DescriptionRequestMessages can be sent to this endpoint via POST request."))))
						.build();
				endpoints.add(e3);
				ConnectorEndpoint e4 = new ConnectorEndpointBuilder(new URI(config.getString("url") + "/ConnectorEndpoint/" + UUID.randomUUID()))
						._accessURL_(new URI(config.getString("url") + "/about"))
						._endpointInformation_(new ArrayList<>(Arrays.asList(new TypedLiteral("The Connector SelfDescription can be obtained from this endpoint via GET request."))))
						.build();
				endpoints.add(e4);
			} catch (Exception e1) {
				LOGGER.error(e1);
			}
		return endpoints;
	}


	private XMLGregorianCalendar getDate(java.time.Instant createdAt) {

		GregorianCalendar cal1 = new GregorianCalendar();
		cal1.setTimeInMillis(createdAt.toEpochMilli());

		try {
			return DatatypeFactory.newInstance().newXMLGregorianCalendar(cal1);
		} catch (DatatypeConfigurationException e) {
			LOGGER.error(e);
		}
		return null;
	}

	private ArrayList<TypedLiteral> getKeyWords(Dataset da) {
		ArrayList<TypedLiteral> keywords = new ArrayList<>();
		for (String tag : da.getTags()) {
			if(!tag.isEmpty()) {
				keywords.add(new TypedLiteral(tag));
			}
		}
		return keywords.isEmpty()? null : keywords;
	}

	private void createRejectionMessage(JsonObject config, URI uri,RejectionReason rejectionReason,Handler<AsyncResult<RejectionMessage>> resultHandler) {
					getJWT(jwtReply -> {
						if (jwtReply.succeeded()) {
							try {
								RejectionMessage message = new RejectionMessageBuilder(new URI(config.getString("url") + "/RejectionMessage/" + UUID.randomUUID()))
									._correlationMessage_(uri)
									._issued_(getDate())
									._modelVersion_(INFO_MODEL_VERSION)
									._issuerConnector_(new URI(config.getString("url") + "#Connector"))
									._securityToken_(new DynamicAttributeTokenBuilder()
											._tokenFormat_(TokenFormat.JWT)
											._tokenValue_(jwtReply.result())
											.build())
									._rejectionReason_(rejectionReason)
									.build();
								resultHandler.handle(Future.succeededFuture(message));
							} catch(URISyntaxException e){
								LOGGER.error(e);
								resultHandler.handle(Future.failedFuture(e));
							}
					} else {
						LOGGER.error(jwtReply.cause());
						resultHandler.handle(Future.failedFuture(jwtReply.cause()));
					}
				});
	}

	public void handleDataMessage(URI uri, Future<Message> header, Future payload, long assetId, Handler<AsyncResult<HttpEntity>> resultHandler) {
		CompositeFuture.all(header,payload).onComplete( reply -> {
			if(reply.succeeded()) {
				String message = null;

				try {
					message = serializer.serialize(header.result());
				} catch (IOException e) {
					LOGGER.error(e);
					handleRejectionMessage(uri, RejectionReason.INTERNAL_RECIPIENT_ERROR, resultHandler);
				}
				final String finalMessage = message;
				getFileName(assetId, fileNameReply -> {
					if(fileNameReply.succeeded()) {
						MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
								.setCharset(StandardCharsets.UTF_8)
								.setContentType(ContentType.MULTIPART_FORM_DATA)
								.addPart("header", new StringBody(finalMessage, ContentType.create("application/json", StandardCharsets.UTF_8)))
								.addBinaryBody("payload", (File) payload.result(), ContentType.create("application/octet-stream"), fileNameReply.result());
						resultHandler.handle(Future.succeededFuture(multipartEntityBuilder.build()));
						((File) payload.result()).delete();
					} else {
						MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
								.setCharset(StandardCharsets.UTF_8)
								.setContentType(ContentType.MULTIPART_FORM_DATA)
								.addPart("header", new StringBody(finalMessage, ContentType.create("application/json", StandardCharsets.UTF_8)))
								.addBinaryBody("payload", (File) payload.result(), ContentType.create("application/octet-stream"), UUID.randomUUID().toString());
						resultHandler.handle(Future.succeededFuture(multipartEntityBuilder.build()));
						((File) payload.result()).delete();
					}
				});
			}else{
				handleRejectionMessage(uri,RejectionReason.INTERNAL_RECIPIENT_ERROR,resultHandler);
				LOGGER.error(reply.cause());
				}
			});
	}

	public void handleAboutMessage(URI uri, Future<Message> header, Future<Connector> payload, Handler<AsyncResult<HttpEntity>> resultHandler) {
		CompositeFuture.all(header, payload).onComplete( reply -> {
			if (reply.succeeded()) {

                try {
					String message = serializer.serialize(header.result());
					String connector = serializer.serialize(payload.result());

					MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
							.setCharset(StandardCharsets.UTF_8)
							.setContentType(ContentType.MULTIPART_FORM_DATA)
							.addPart("header", new StringBody(message, ContentType.create("application/json", StandardCharsets.UTF_8)))
							.addPart("payload", new StringBody(connector, ContentType.create("application/json", StandardCharsets.UTF_8)));
					resultHandler.handle(Future.succeededFuture(multipartEntityBuilder.build()));
				} catch (IOException e) {
					LOGGER.error(e);
					handleRejectionMessage(uri, RejectionReason.INTERNAL_RECIPIENT_ERROR, resultHandler);
				}
			} else {
				handleRejectionMessage(uri, RejectionReason.INTERNAL_RECIPIENT_ERROR, resultHandler);
				LOGGER.error(reply.cause());
			}
		});
	}

	public void getFileName(Long id, Handler<AsyncResult<String>> result){
		dataAssetManager.findDistributionById(id,jsonObjectAsyncResult -> {
			if (jsonObjectAsyncResult.succeeded()){
				Distribution dataAsset = Json.decodeValue(jsonObjectAsyncResult.result().toString(), Distribution.class);
				result.handle(Future.succeededFuture(dataAsset.getFilename()));
			}
			else {
				LOGGER.error(jsonObjectAsyncResult.cause());
				result.handle(Future.failedFuture(jsonObjectAsyncResult.cause()));
			}
		});
	}

	public void handleRejectionMessage(URI uri,RejectionReason rejectionReason,Handler<AsyncResult<HttpEntity>> resultHandler) {
		configService.getConfiguration(configReply -> {
			if(configReply.succeeded()) {
				createRejectionMessage(configReply.result(),uri, rejectionReason, rejectionMessageAsyncResult -> {
					if (rejectionMessageAsyncResult.succeeded()) {
						HttpEntity reject = createMultipartMessage(rejectionMessageAsyncResult.result());
						resultHandler.handle(Future.succeededFuture(reject));
					} else {
						resultHandler.handle(Future.failedFuture(rejectionMessageAsyncResult.cause()));
					}
				});
			} else {
				LOGGER.error(configReply.cause());
				resultHandler.handle(Future.failedFuture(configReply.cause()));
			}
		});
	}

	private HttpEntity createMultipartMessage(Message message) {
		ContentBody cb = null;
		try {
			cb = new StringBody(serializer.serialize(message), ContentType.create("application/json", StandardCharsets.UTF_8));
		} catch (IOException e) {
			e.printStackTrace();
		}

		MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
				.setCharset(StandardCharsets.UTF_8)
				.setContentType(ContentType.MULTIPART_FORM_DATA)
				.addPart("header", cb);

			return multipartEntityBuilder.build();
	}

	private void getJWT(Handler<AsyncResult<String>> resultHandler){
		authAdapterService.retrieveToken(tokenReply -> resultHandler.handle(tokenReply));
	}
}
