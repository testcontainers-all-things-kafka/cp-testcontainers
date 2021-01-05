package net.christophschubert.cp.testcontainers;

public class SecurityConfigs {
    public static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    public static final String PLAIN = "PLAIN";

    static final String OAUTHBEARER = "OAUTHBEARER";

    public static String oauthJaas(String username, String password, String url) {
        return String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                " username=\"%s\" password=\"%s\" metadataServerUrls=\"%s\";",
        username, password, url);
    }
}
