package de.fraunhofer.fokus.ids.persistence.managers;

import de.fraunhofer.fokus.ids.persistence.entities.User;
import de.fraunhofer.fokus.ids.persistence.util.DatabaseConnector;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.sqlclient.Tuple;
import org.mindrot.jbcrypt.BCrypt;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class AuthManager {

    private Logger LOGGER = LoggerFactory.getLogger(AuthManager.class.getName());
    private JWTAuth provider;

    private DatabaseConnector databaseConnector = DatabaseConnector.getInstance();

    private static final String USER_QUERY = "SELECT * FROM public.user WHERE username = $1";

    public AuthManager(Vertx vertx) {
        provider = JWTAuth.create(vertx, new JWTAuthOptions()
                .addPubSecKey(new PubSecKeyOptions()
                        .setAlgorithm("HS256")
                        .setPublicKey("478f66d6ab103ddb45d4f37fb9ee4d34")
                        .setSymmetric(true)));

    }

    public JWTAuth getProvider(){
        return provider;
    }

    public void login(JsonObject credentials, Handler<AsyncResult<String>> resultHandler) {

        databaseConnector.query(USER_QUERY, Tuple.tuple().addString(credentials.getString("username")),reply -> {
            if (reply.failed()) {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause().toString()));
            } else {
                if (reply.result().size() > 0) {
                    User user = Json.decodeValue(reply.result().get(0).toString(), User.class);

                    if (BCrypt.checkpw(credentials.getString("password"), user.getPassword())) {
                        resultHandler.handle(Future.succeededFuture(provider.generateToken(new JsonObject().put("sub", user.getUsername()), new JWTOptions().setExpiresInMinutes(24*60))));
                    } else {
                        resultHandler.handle(Future.failedFuture("Password is not identical to password in database."));
                    }
                }
                else {
                    resultHandler.handle(Future.failedFuture("User is not registered in the database."));
                }
            }
        });

    }

}
