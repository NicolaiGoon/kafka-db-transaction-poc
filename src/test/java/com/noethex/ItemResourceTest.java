package com.noethex;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

@QuarkusTest
class ItemResourceTest {

    @Test
    void createPooledReturns201() {
        create("/items/pooled", "widget");
    }

    @Test
    void createChainedReturns201() {
        create("/items/chained", "gadget");
    }

    private void create(String path, String name) {
        given()
                .contentType("application/json")
                .body("{\"name\":\"" + name + "\",\"payload\":\"hello\"}")
                .when().post(path)
                .then()
                .statusCode(201)
                .body("id", notNullValue())
                .body("name", is(name))
                .body("payload", is("hello"));
    }
}
