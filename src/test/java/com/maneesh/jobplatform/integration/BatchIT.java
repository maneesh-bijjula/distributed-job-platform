package com.maneesh.jobplatform.integration;

import com.maneesh.jobplatform.auth.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={
        "jobs.dispatcher.enabled=false","jobs.worker.enabled=false"})
@DirtiesContext(classMode=DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureObservability
class BatchIT extends AuthenticatedIntegrationTest {
    @Autowired JdbcTemplate jdbc;
    @Autowired ApiKeyService keys;
    @Autowired io.micrometer.core.instrument.MeterRegistry registry;
    @LocalServerPort int port;
    private final TestRestTemplate client=new TestRestTemplate();
    @BeforeEach void reset() { IntegrationDatabase.reset(jdbc); }

    private record Actor(UUID owner,String token) {}
    private Actor actor(int quota) {
        var operator=keys.authenticate(OPERATOR_TOKEN).orElseThrow();
        var owner=keys.createOwner(operator,"Batch integration",quota);
        return new Actor(owner.id(),keys.issue(operator,owner.id(),"Batch client",ApiPrincipal.Role.USER).token());
    }
    private Map<String,Object> item(String key,String text) {
        return Map.of("idempotencyKey",key,"job",Map.of("type","UPPERCASE","text",text));
    }
    private ResponseEntity<com.fasterxml.jackson.databind.JsonNode> submit(Actor actor,List<Map<String,Object>> items) {
        var headers=new HttpHeaders();headers.setBearerAuth(actor.token());
        return client.exchange("http://localhost:"+port+"/api/v1/jobs/batch",HttpMethod.POST,
                new HttpEntity<>(Map.of("items",items),headers),com.fasterxml.jackson.databind.JsonNode.class);
    }

    @Test void mixedQuotaResultsRetainEachCommittedJobAndReplayRemainsFreeAtLimit() throws InterruptedException {
        double second=jdbc.queryForObject("SELECT extract(second FROM clock_timestamp())::double precision",Double.class);
        if(second>55) Thread.sleep((long)((60-second+.1)*1000));
        var actor=actor(2);
        var items=List.of(item("first","one"),item("second","two"),item("third","three"));
        var response=submit(actor,items);
        assertThat(response.getStatusCode().value()).isEqualTo(207);
        var body=response.getBody();
        assertThat(body.get("created").asInt()).isEqualTo(2);
        assertThat(body.get("unsuccessful").asInt()).isEqualTo(1);
        assertThat(body.at("/items/2/status").asInt()).isEqualTo(429);
        assertThat(body.at("/items/2/retryAfterSeconds").asInt()).isBetween(1,60);
        var replay=submit(actor,items).getBody();
        assertThat(replay.get("created").asInt()).isZero();
        assertThat(replay.get("replayed").asInt()).isEqualTo(2);
        assertThat(replay.at("/items/0/job/id")).isEqualTo(body.at("/items/0/job/id"));
        assertThat(jdbc.queryForObject("SELECT used FROM submission_usage WHERE owner_id=?",Integer.class,actor.owner())).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs WHERE owner_id=?",Integer.class,actor.owner())).isEqualTo(2);
    }

    @Test void allSuccessAndAllReplayStatusesMatchSingleJobSemantics() {
        var actor=actor(5);
        var items=List.of(item("a","one"),item("b","two"));
        assertThat(submit(actor,items).getStatusCode().value()).isEqualTo(202);
        assertThat(submit(actor,items).getStatusCode().value()).isEqualTo(200);
        var response=submit(actor,List.of(item("a","different"),item("c","three")));
        assertThat(response.getStatusCode().value()).isEqualTo(207);
        assertThat(response.getBody().at("/items/0/status").asInt()).isEqualTo(409);
        assertThat(response.getBody().at("/items/1/status").asInt()).isEqualTo(202);
    }

    @Test void identicalKeysAcrossOwnersRemainIsolated() {
        var a=actor(5);var b=actor(5);
        var items=List.of(item("shared","same text"));
        var first=submit(a,items).getBody().at("/items/0/job");
        var second=submit(b,items).getBody().at("/items/0/job");
        assertThat(first.get("id")).isNotEqualTo(second.get("id"));
        assertThat(first.get("ownerId").asText()).isEqualTo(a.owner().toString());
        assertThat(second.get("ownerId").asText()).isEqualTo(b.owner().toString());
    }

    @Test void oneBeforeCommitFailureRollsBackOnlyThatItemAndSanitizesItsError() {
        var actor=actor(5);
        double failures=registry.get("job.platform.batch.items").tag("status","503").counter().count();
        jdbc.execute("CREATE FUNCTION reject_batch_metric() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_type='SHA256' THEN RAISE EXCEPTION 'private batch database error'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER reject_batch_metric BEFORE INSERT ON job_metric_counts FOR EACH ROW EXECUTE FUNCTION reject_batch_metric()");
        try {
            var failure=Map.<String,Object>of("idempotencyKey","failed","job",Map.of("type","SHA256","text","failure"));
            var response=submit(actor,List.of(item("first","one"),failure,item("third","three")));
            assertThat(response.getStatusCode().value()).isEqualTo(207);
            assertThat(response.getBody().at("/items/1/status").asInt()).isEqualTo(503);
            assertThat(response.getBody().at("/items/2/status").asInt()).isEqualTo(202);
            assertThat(response.getBody().toString()).doesNotContain("private batch database error");
            assertThat(registry.get("job.platform.batch.items").tag("status","503").counter().count()).isEqualTo(failures+1);
            for(String table:List.of("jobs","job_events","job_outbox"))
                assertThat(jdbc.queryForObject("SELECT count(*) FROM "+table,Integer.class)).isEqualTo(2);
            assertThat(jdbc.queryForObject("SELECT used FROM submission_usage WHERE owner_id=?",Integer.class,actor.owner())).isEqualTo(2);
        } finally { jdbc.execute("DROP TRIGGER reject_batch_metric ON job_metric_counts");jdbc.execute("DROP FUNCTION reject_batch_metric()"); }
        assertThat(submit(actor,List.of(Map.<String,Object>of("idempotencyKey","failed","job",Map.of("type","SHA256","text","failure"))))
                .getStatusCode().value()).isEqualTo(202);
    }
}
