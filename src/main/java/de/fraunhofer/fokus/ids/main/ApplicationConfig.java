package de.fraunhofer.fokus.ids.main;

import io.vertx.core.json.JsonObject;

public class ApplicationConfig {

    private static final String ROUTE_PREFIX = "de.fraunhofer.fokus.ids.";
    public static final String DATABASE_SERVICE = ROUTE_PREFIX + "databaseService";
  
    public static final String ENV_INIT = "INIT";
    public static final String DEFAULT_INIT = "true";
    
    public static final String ENV_REPOSITORY = "REPOSITORY";
    public static final String DEFAULT_REPOSITORY = "/ids/repo/db";
    
    public static final String ENV_FRONTEND_CONFIG = "FRONTEND_CONFIG";
    public static final JsonObject DEFAULT_FRONTEND_CONFIG = new JsonObject("{\"username\":\"admin\",\"password\":\"admin\"}");
    
    public static final String ENV_AUTH_CONFIG = "AUTH_CONFIG";
    public static final JsonObject DEFAULT_AUTH_CONFIG = new JsonObject("{\"mode\":\"skip\",\"keystorename\":\"my-keystore.jks\", \"keystorepassword\":\"my-password\", \"keystorealias\":\"connector-alias\",\"truststorename\":\"truststore-name\",\"dapsurl\":\"https://daps.aisec.fraunhofer.de/v2/\", \"dapsissuer\":\"https://daps.aisec.fraunhofer.de\"}");
    
    public static final String ENV_DB_CONFIG = "DB_CONFIG";
    public static final JsonObject DEFAULT_DB_CONFIG = new JsonObject("{\"host\":\"localhost\",\"port\":5432,\"database\":\"ids\",\"user\":\"ids\",\"password\":\"ids\"}");
    
    public static final String ENV_API_KEY = "API_KEY";
    public static final String DEFAULT_API_KEY = "yourapikey";
    
    public static final String ENV_SERVICE_PORT = "SERVICE_PORT";
    public static final Integer DEFAULT_SERVICE_PORT = 8080;
    
    
   
    
    
    
    
}