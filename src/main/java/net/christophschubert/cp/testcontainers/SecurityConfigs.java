package net.christophschubert.cp.testcontainers;

import java.util.Map;

public class SecurityConfigs {
    public static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    public static final String PLAIN = "PLAIN";
    public static final String BASIC = "BASIC";
    public static final String USER_INFO = "USER_INFO";
    public static final String OAUTHBEARER = "OAUTHBEARER";

    public static String oauthJaas(String username, String password, String url) {
        return String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "username=\"%s\" password=\"%s\" metadataServerUrls=\"%s\";",
        username, password, url);
    }

    static public Map<String, Object> oAuthWithTokenCallbackHandlerProperties(String principal, String secret, String mdsBootstrap) {
        return Map.of(
                "security.protocol", SASL_PLAINTEXT,
                "sasl.mechanism", OAUTHBEARER,
                "sasl.login.callback.handler.class", "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler",
                "sasl.jaas.config", oauthJaas(principal, secret, mdsBootstrap)
        );
    }
}
