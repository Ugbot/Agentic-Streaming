package org.jagentic.tools.app;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

/** Verifies the REST facade and the MCP-HTTP endpoint over the utility pack (the default
 * pack selection). RestAssured drives a booted Quarkus instance. */
@QuarkusTest
class ToolServiceEndpointsTest {

  static final String TOKEN = "test-tool-services-token";

  @Test
  void restListsToolsWithSchemas() {
    given().header("Authorization", "Bearer " + TOKEN)
        .when().get("/tools")
        .then().statusCode(200)
        .body("name", hasItem("util_add"))
        .body("find { it.name == 'util_add' }.inputSchema.properties.a.type", equalTo("number"));
  }

  @Test
  void restInvokesTool() {
    given().header("Authorization", "Bearer " + TOKEN).contentType(ContentType.JSON).body("{\"a\":40,\"b\":2}")
        .when().post("/tools/util_add")
        .then().statusCode(200)
        .body("ok", is(true))
        .body("result", equalTo(42.0f));
  }

  @Test
  void restUnknownToolIs404() {
    given().header("Authorization", "Bearer " + TOKEN).contentType(ContentType.JSON).body("{}")
        .when().post("/tools/nope")
        .then().statusCode(404).body("ok", is(false));
  }

  @Test
  void mcpHttpListsAndCallsTools() {
    // tools/list
    given().header("Authorization", "Bearer " + TOKEN).contentType(ContentType.JSON)
        .body("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}")
        .when().post("/mcp")
        .then().statusCode(200)
        .body("result.tools.name", hasItem("util_toUpperCase"));

    // tools/call
    given().header("Authorization", "Bearer " + TOKEN).contentType(ContentType.JSON)
        .body("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":"
            + "{\"name\":\"util_toUpperCase\",\"arguments\":{\"text\":\"hi\"}}}")
        .when().post("/mcp")
        .then().statusCode(200)
        .body("result.isError", is(false))
        .body("result.content[0].text", equalTo("HI"));
  }

  @Test
  void webPackIsServedToo() {
    // the web pack's tools are listed alongside util (default selection = all packs)
    given().header("Authorization", "Bearer " + TOKEN).when().get("/tools").then().statusCode(200)
        .body("name", hasItem("web_fetch"))
        .body("name", hasItem("doc_extract"));
    // doc_extract works over REST with no network (inline HTML -> Jsoup extraction)
    given().header("Authorization", "Bearer " + TOKEN).contentType(ContentType.JSON)
        .body("{\"text\":\"<html><head><title>Hi</title></head><body>hello world</body></html>\","
            + "\"content_type\":\"text/html\"}")
        .when().post("/tools/doc_extract")
        .then().statusCode(200)
        .body("ok", is(true))
        .body("result.title", equalTo("Hi"));
  }

  @Test
  void mcpHttpInitializeAdvertisesProtocol() {
    given().header("Authorization", "Bearer " + TOKEN).contentType(ContentType.JSON)
        .body("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}")
        .when().post("/mcp")
        .then().statusCode(200)
        .body("result.protocolVersion", equalTo("2024-11-05"));
  }

  @Test
  void restRejectsMissingToken() {
    io.restassured.RestAssured.given()
        .when().get("/tools")
        .then().statusCode(401).body("ok", is(false));
  }

  @Test
  void restAndMcpRejectWrongToken() {
    String wrong = "Bearer " + java.util.UUID.randomUUID();
    io.restassured.RestAssured.given().header("Authorization", wrong)
        .contentType(ContentType.JSON).body("{\"a\":1,\"b\":2}")
        .when().post("/tools/util_add")
        .then().statusCode(401);
    io.restassured.RestAssured.given().header("Authorization", wrong)
        .contentType(ContentType.JSON)
        .body("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}")
        .when().post("/mcp")
        .then().statusCode(401);
  }

  @Test
  void swaggerUiIsNotServedOutsideDev() {
    io.restassured.RestAssured.given().header("Authorization", "Bearer " + TOKEN)
        .when().get("/q/swagger-ui")
        .then().statusCode(404);
  }
}
