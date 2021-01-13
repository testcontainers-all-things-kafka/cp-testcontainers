package net.christophschubert.cp.testcontainers;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

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

    public static String plainJaas(String username, String password) {
        return plainJaas(username, password, Collections.emptyMap());
    }



    public static String plainJaas(String user, String password, Map<String, String> additionalUsers) {
        final var collectUsers = additionalUsers.entrySet().stream()
                .map(e -> String.format("user_%s=\"%s\"", e.getKey(), e.getValue()))
                .collect(Collectors.joining(" "));
        return String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\" %s;",
                user, password, collectUsers);
    }




    static public Map<String, Object> oAuthWithTokenCallbackHandlerProperties(String principal, String secret, String mdsBootstrap) {
        assert (mdsBootstrap.startsWith("http"));
        return Map.of(
                "security.protocol", SASL_PLAINTEXT,
                "sasl.mechanism", OAUTHBEARER,
                "sasl.login.callback.handler.class", "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler",
                "sasl.jaas.config", oauthJaas(principal, secret, mdsBootstrap)
        );
    }

    static public Map<String, Object> plainJaasProperties(String principal, String password) {
        return Map.of(
                "sasl.mechanism", PLAIN,
                "security.protocol", SASL_PLAINTEXT,
                "sasl.jaas.config", plainJaas(principal, password)
        );
    }

    static public Map<String, Object> confluentMdsSettings(String principal, String secret, String mdsBootstrap) {
        assert (mdsBootstrap.startsWith("http"));
        return Map.of(
                "confluent.metadata.bootstrap.server.urls", mdsBootstrap,
                "confluent.metadata.http.auth.credentials.provider", BASIC,
                "confluent.metadata.basic.auth.user.info", principal + ":" + secret
        );
        //TODO some of the different examples still mention USER_INFO, double check whether that's still necessary.
    }

}
