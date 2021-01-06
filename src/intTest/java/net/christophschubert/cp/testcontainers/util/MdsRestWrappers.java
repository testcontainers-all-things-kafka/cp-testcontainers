package net.christophschubert.cp.testcontainers.util;

import static io.restassured.RestAssured.given;

public class MdsRestWrappers {
    // all methods in the class assume that MDS is listening on localhost and that
    // RestAssured.port has been set to the MDS port

    static public String getKafkaClusterId() {
        return given().
                when().
                get("/v1/metadata/id").
                then().
                statusCode(200).
                log().all().extract().body().path("id").toString();
    }

}
