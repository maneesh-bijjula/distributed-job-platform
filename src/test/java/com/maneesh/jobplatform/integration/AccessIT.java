package com.maneesh.jobplatform.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.JobPlatformApplication;
import com.maneesh.jobplatform.auth.*;
import com.maneesh.jobplatform.auth.AccessRecords.*;
import com.maneesh.jobplatform.deadletter.DeadLetterService;
import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.worker.ExecutionService;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={"jobs.dispatcher.enabled=false","jobs.worker.enabled=false"})
@DirtiesContext(classMode=DirtiesContext.ClassMode.AFTER_CLASS)
class AccessIT extends AuthenticatedIntegrationTest {
    @LocalServerPort int port;
    @Autowired JdbcTemplate jdbc;
    @Autowired ApiKeyService keys;
    @Autowired JobService jobs;
    @Autowired ExecutionService execution;
    @Autowired DeadLetterService inbox;
    @MockitoSpyBean AccessRepository access;
    @MockitoSpyBean JobRepository repository;
    final TestRestTemplate client = new TestRestTemplate();
    ApiPrincipal operator;

    @BeforeEach void reset() {
        IntegrationDatabase.reset(jdbc);
        operator=keys.authenticate(OPERATOR_TOKEN).orElseThrow();
        await().atMost(Duration.ofSeconds(15)).until(() -> jdbc.queryForObject("SELECT extract(second FROM clock_timestamp())<50",Boolean.class));
    }
    private IssuedKey user(int limit) {
        Owner owner=keys.createOwner(operator,"Test owner",limit);
        return keys.issue(operator,owner.id(),"Test client",ApiPrincipal.Role.USER);
    }
    private ResponseEntity<JsonNode> request(String token,HttpMethod method,String path,Object body) {
        return request(port,token,method,path,body,null);
    }
    private ResponseEntity<JsonNode> request(int apiPort,String token,HttpMethod method,String path,Object body,String key) {
        HttpHeaders headers=new HttpHeaders();
        if(token!=null) headers.setBearerAuth(token);
        if(key!=null) headers.set("Idempotency-Key",key);
        return client.exchange("http://localhost:"+apiPort+path,method,new HttpEntity<>(body,headers),JsonNode.class);
    }
    private ResponseEntity<JsonNode> submit(String token,String key) {
        return request(port,token,HttpMethod.POST,"/api/v1/jobs",Map.of("type","UPPERCASE","text","private owner input",
                "scheduledAt",Instant.now().plusSeconds(3600).truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString()),key);
    }
    private Job owned(IssuedKey key) {
        return jobs.submitFor(key.key().ownerId(),JobType.UPPERCASE,"hello",null,0,null,30,null).job();
    }

    @ParameterizedTest
    @ValueSource(strings={"/api/v1/jobs","/api/v1/jobs/00000000-0000-0000-0000-000000000002/events",
            "/api/v1/dead-letters","/api/v1/admin/owners","/api/v1/auth/me","/actuator/prometheus","/actuator/info"})
    void anonymousRequestsAreRejectedWithBearerChallenge(String path) {
        var response=request(null,HttpMethod.GET,path,null);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getHeaders().getFirst("WWW-Authenticate")).isEqualTo("Bearer");
        assertThat(response.getBody().toString()).doesNotContain("private","token_hash");
    }
    @Test void healthIsPublicButEnvironmentAndConfigAreInaccessible() {
        assertThat(request(null,HttpMethod.GET,"/actuator/health",null).getStatusCode()).isEqualTo(HttpStatus.OK);
        for(String path:new String[]{"/actuator/env","/actuator/configprops","/actuator/beans"})
            assertThat(request(OPERATOR_TOKEN,HttpMethod.GET,path,null).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }
    @Test void invalidOrAmbiguousCredentialsAreRejectedAndNoSessionIsCreated() {
        for(String token:new String[]{"", "bad", KeyTokens.generate()})
            assertThat(request(token,HttpMethod.GET,"/api/v1/jobs",null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        var headers=new HttpHeaders();
        headers.add("Authorization","Bearer "+OPERATOR_TOKEN); headers.add("Authorization","Bearer "+OPERATOR_TOKEN);
        assertThat(client.exchange("http://localhost:"+port+"/api/v1/jobs",HttpMethod.GET,new HttpEntity<>(headers),JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        var valid=request(OPERATOR_TOKEN,HttpMethod.GET,"/api/v1/auth/me",null);
        assertThat(valid.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(valid.getHeaders().get("Set-Cookie")).isNull();
        assertThat(request(null,HttpMethod.GET,"/api/v1/jobs?api_key="+OPERATOR_TOKEN,null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
    @Test void ownersCannotReadOrMutateEachOthersJobsAcrossEveryEndpoint() {
        IssuedKey alice=user(120),bob=user(120);
        Job source=owned(alice);
        String path="/api/v1/jobs/"+source.id();
        for(String suffix:new String[]{"","/events","/attempts"}) {
            assertThat(request(alice.token(),HttpMethod.GET,path+suffix,null).getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(request(bob.token(),HttpMethod.GET,path+suffix,null).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
            assertThat(request(OPERATOR_TOKEN,HttpMethod.GET,path+suffix,null).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        }
        for(String suffix:new String[]{"/cancel","/retry"})
            assertThat(request(port,bob.token(),HttpMethod.POST,path+suffix,null,"cross-owner").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(jobs.get(source.id()).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(request(bob.token(),HttpMethod.GET,"/api/v1/jobs",null).getBody().get("items")).isEmpty();
        assertThat(request(alice.token(),HttpMethod.GET,"/api/v1/jobs?status=QUEUED",null).getBody().get("items")).hasSize(1);
        assertThat(request(bob.token(),HttpMethod.GET,"/api/v1/jobs?ownerId="+alice.key().ownerId(),null).getBody().get("items")).isEmpty();
    }
    @Test void ownerCannotBeSuppliedOrSpoofedInSubmissionJson() {
        var user=user(120);
        var response=request(user.token(),HttpMethod.POST,"/api/v1/jobs",Map.of("type","SHA256","text","hello","ownerId",ApiPrincipal.LEGACY_OWNER));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
    @Test void idempotencyIsScopedByOwnerButSharedAcrossKeysOfOneOwner() {
        var alice=user(120); var bob=user(120);
        var second=keys.issue(operator,alice.key().ownerId(),"Second client",ApiPrincipal.Role.USER);
        Map<String,Object> body=Map.of("type","SHA256","text","hello");
        var first=request(port,alice.token(),HttpMethod.POST,"/api/v1/jobs",body,"shared-key");
        var replay=request(port,second.token(),HttpMethod.POST,"/api/v1/jobs",body,"shared-key");
        var other=request(port,bob.token(),HttpMethod.POST,"/api/v1/jobs",body,"shared-key");
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replay.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        assertThat(other.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(other.getBody().get("id")).isNotEqualTo(first.getBody().get("id"));
        assertThat(request(port,alice.token(),HttpMethod.POST,"/api/v1/jobs",Map.of("type","SHA256","text","changed"),"shared-key").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }
    @ParameterizedTest
    @ValueSource(strings={"/api/v1/admin/owners","/api/v1/admin/audit","/api/v1/dead-letters","/actuator/info","/actuator/prometheus"})
    void ordinaryKeysCannotUseOperatorReadEndpoints(String path) {
        assertThat(request(user(120).token(),HttpMethod.GET,path,null).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }
    @Test void ordinaryKeysCannotUseAnyOperatorMutation() {
        var user=user(120);
        for(String path:new String[]{"/api/v1/admin/owners","/api/v1/admin/owners/"+user.key().ownerId()+"/keys",
                "/api/v1/admin/owners/"+user.key().ownerId()+"/limit","/api/v1/admin/keys/"+user.key().id()+"/revoke",
                "/api/v1/dead-letters/collect","/api/v1/dead-letters/"+UUID.randomUUID()+"/recover",
                "/api/v1/dead-letters/"+UUID.randomUUID()+"/discard"})
            assertThat(request(user.token(),HttpMethod.POST,path,Map.of()).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }
    @Test void revokedKeysFailOnNextRequestWhileOtherOwnerKeysKeepWorking() {
        var first=user(120);
        var second=keys.issue(operator,first.key().ownerId(),"Rotation",ApiPrincipal.Role.USER);
        assertThat(request(first.token(),HttpMethod.GET,"/api/v1/auth/me",null).getStatusCode()).isEqualTo(HttpStatus.OK);
        var revoked=request(OPERATOR_TOKEN,HttpMethod.POST,"/api/v1/admin/keys/"+first.key().id()+"/revoke",null);
        assertThat(revoked.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(request(OPERATOR_TOKEN,HttpMethod.POST,"/api/v1/admin/keys/"+first.key().id()+"/revoke",null).getBody()).isEqualTo(revoked.getBody());
        assertThat(request(first.token(),HttpMethod.GET,"/api/v1/auth/me",null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(request(second.token(),HttpMethod.GET,"/api/v1/auth/me",null).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM access_audit WHERE action='KEY_REVOKED' AND key_id=?",Integer.class,first.key().id())).isEqualTo(1);
    }
    @Test void keyHashesAreStoredWithoutRawTokensAndIssuanceIsAudited() {
        var issued=user(120);
        String stored=jdbc.queryForObject("SELECT token_hash FROM api_keys WHERE id=?",String.class,issued.key().id());
        assertThat(stored).isEqualTo(KeyTokens.hash(issued.token())).doesNotContain(issued.token());
        var response=request(OPERATOR_TOKEN,HttpMethod.GET,"/api/v1/admin/owners/"+issued.key().ownerId()+"/keys",null);
        assertThat(response.getBody().toString()).doesNotContain(stored,issued.token(),"tokenHash","token_hash");
        assertThat(issued.toString()).doesNotContain(issued.token());
        assertThat(jdbc.queryForObject("SELECT actor_key_id FROM access_audit WHERE action='KEY_CREATED' AND key_id=?",UUID.class,issued.key().id())).isEqualTo(operator.keyId());
    }
    @Test void bootstrapIsIdempotentAndNeverResurrectsARevokedKey() {
        String token=KeyTokens.generate();
        keys.bootstrap(token);
        var first=keys.authenticate(token).orElseThrow();
        keys.bootstrap(token);
        assertThat(keys.authenticate(token).orElseThrow()).isEqualTo(first);
        keys.revoke(operator,first.keyId());
        keys.bootstrap(token);
        assertThat(keys.authenticate(token)).isEmpty();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM access_audit WHERE action='BOOTSTRAP_KEY_CREATED' AND key_id=?",Integer.class,first.keyId())).isEqualTo(1);
    }
    @Test void databaseRejectsCrossOwnerRetryLinks() {
        Job alice=owned(user(120)),bob=owned(user(120));
        assertThatThrownBy(() -> jdbc.update("UPDATE jobs SET retry_of=? WHERE id=?",alice.id(),bob.id()))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }
    @Test void authenticationStorageFailureFailsClosedWithoutLeakingDetails() {
        doThrow(new DataAccessResourceFailureException("private database credential")).when(access).authenticate(anyString());
        var response=request(OPERATOR_TOKEN,HttpMethod.GET,"/api/v1/jobs",null);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().toString()).doesNotContain("private database credential");
    }
    @Test void keyAndOwnerCreationRollBackWhenAuditCannotCommit() {
        var user=user(120);
        int count=jdbc.queryForObject("SELECT count(*) FROM api_keys WHERE owner_id=?",Integer.class,user.key().ownerId());
        doThrow(new DataAccessResourceFailureException("audit unavailable")).when(access).audit(any(),anyString(),any(),any());
        assertThatThrownBy(() -> keys.issue(operator,user.key().ownerId(),"rollback",ApiPrincipal.Role.USER)).isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM api_keys WHERE owner_id=?",Integer.class,user.key().ownerId())).isEqualTo(count);
    }
    @Test void replayIsFreeAndQuotaRejectionLeavesNoJobOrOutboxBehind() {
        var user=user(1);
        Map<String,Object> body=Map.of("type","SHA256","text","hello");
        var first=request(port,user.token(),HttpMethod.POST,"/api/v1/jobs",body,"quota-key");
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(request(port,user.token(),HttpMethod.POST,"/api/v1/jobs",body,"quota-key").getStatusCode()).isEqualTo(HttpStatus.OK);
        var denied=request(port,user.token(),HttpMethod.POST,"/api/v1/jobs",body,"new-key");
        assertThat(denied.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(Integer.parseInt(denied.getHeaders().getFirst("Retry-After"))).isBetween(1,60);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs WHERE owner_id=?",Integer.class,user.key().ownerId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT used FROM submission_usage WHERE owner_id=?",Integer.class,user.key().ownerId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_outbox",Integer.class)).isEqualTo(1);
    }
    @Test void quotaIsSharedAcrossOwnerKeysAndResetsInANewDatabaseMinute() {
        var user=user(1); var second=keys.issue(operator,user.key().ownerId(),"Other key",ApiPrincipal.Role.USER);
        assertThat(submit(user.token(),"first").getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(submit(second.token(),"second").getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        jdbc.update("UPDATE submission_usage SET window_start=window_start-interval '1 minute' WHERE owner_id=?",user.key().ownerId());
        assertThat(submit(second.token(),"second").getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(jdbc.queryForObject("SELECT used FROM submission_usage WHERE owner_id=?",Integer.class,user.key().ownerId())).isEqualTo(1);
        assertThat(submit(user(1).token(),"independent").getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }
    @Test void submissionAuditFailureAlsoRollsBackQuota() {
        var user=user(1);
        doThrow(new DataAccessResourceFailureException("audit unavailable")).when(repository).recordEvent(any(),eq(JobEvent.Type.SUBMITTED),any(),any());
        assertThat(submit(user.token(),"rollback").getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM submission_usage WHERE owner_id=?",Integer.class,user.key().ownerId())).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs WHERE owner_id=?",Integer.class,user.key().ownerId())).isZero();
    }
    @Test void manualRetryRetainsOwnerAndConsumesAdmissionOnlyOnNewChild() {
        var user=user(2); Job job=owned(user);
        assertThat(request(user.token(),HttpMethod.POST,"/api/v1/jobs/"+job.id()+"/cancel",null).getStatusCode()).isEqualTo(HttpStatus.OK);
        var child=request(port,user.token(),HttpMethod.POST,"/api/v1/jobs/"+job.id()+"/retry",null,"retry-child");
        assertThat(child.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(child.getBody().get("ownerId").asText()).isEqualTo(user.key().ownerId().toString());
        assertThat(request(port,user.token(),HttpMethod.POST,"/api/v1/jobs/"+job.id()+"/retry",null,"retry-child").getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(request(port,user.token(),HttpMethod.POST,"/api/v1/jobs/"+job.id()+"/retry",null,"another-child").getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }
    @Test void operatorRecoveryPreservesSourceOwnerAndRecordsActorKey() {
        var user=user(120); Job source=owned(user);
        execution.fail(execution.claim(source.id(),0).orElseThrow());
        UUID id=inbox.list(null,0,10).getFirst().id();
        var recovered=request(port,OPERATOR_TOKEN,HttpMethod.POST,"/api/v1/dead-letters/"+id+"/recover",null,"audited-recovery");
        assertThat(recovered.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(recovered.getBody().get("resolvedByKeyId").asText()).isEqualTo(operator.keyId().toString());
        String target="/api/v1/jobs/"+recovered.getBody().get("recoveryJobId").asText();
        assertThat(request(user.token(),HttpMethod.GET,target,null).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(request(OPERATOR_TOKEN,HttpMethod.GET,target,null).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(jobs.get(UUID.fromString(recovered.getBody().get("recoveryJobId").asText())).ownerId()).isEqualTo(source.ownerId());
    }
    @Test void bootstrapCanReadLegacyJobsButOtherOwnersCannot() {
        Job legacy=jobs.submit(JobType.SHA256,"preserved legacy input","legacy-replay").job();
        assertThat(legacy.ownerId()).isEqualTo(ApiPrincipal.LEGACY_OWNER);
        assertThat(request(OPERATOR_TOKEN,HttpMethod.GET,"/api/v1/jobs/"+legacy.id(),null).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(request(user(120).token(),HttpMethod.GET,"/api/v1/jobs/"+legacy.id(),null).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
    @Test void concurrentApisShareQuotaAndObserveRevocationWithoutCaches() throws Exception {
        var user=user(5); var second=keys.issue(operator,user.key().ownerId(),"Other API",ApiPrincipal.Role.USER);
        try(var context=new SpringApplicationBuilder(JobPlatformApplication.class).web(WebApplicationType.SERVLET)
                .run("--server.port=0","--jobs.dispatcher.enabled=false","--jobs.worker.enabled=false")) {
            int secondPort=((ServletWebServerApplicationContext)context).getWebServer().getPort();
            // Keep this bounded concurrency assertion away from the fixed-window boundary.
            await().atMost(Duration.ofSeconds(15)).until(() -> jdbc.queryForObject("SELECT extract(second FROM clock_timestamp())<50",Boolean.class));
            try(var pool=Executors.newVirtualThreadPerTaskExecutor()) {
                var tasks=new ArrayList<Callable<HttpStatusCode>>();
                for(int i=0;i<20;i++) {
                    int apiPort=i%2==0?port:secondPort;
                    String token=i%2==0?user.token():second.token();
                    tasks.add(() -> request(apiPort,token,HttpMethod.POST,"/api/v1/jobs",Map.of("type","SHA256","text","concurrent"),UUID.randomUUID().toString()).getStatusCode());
                }
                int accepted=0,limited=0;
                for(var result:pool.invokeAll(tasks)) {
                    var status=result.get();
                    if(status==HttpStatus.ACCEPTED) accepted++;
                    else if(status==HttpStatus.TOO_MANY_REQUESTS) limited++;
                    else fail("Unexpected admission response: "+status);
                }
                assertThat(accepted).isEqualTo(5); assertThat(limited).isEqualTo(15);
            }
            assertThat(jdbc.queryForObject("SELECT used FROM submission_usage WHERE owner_id=?",Integer.class,user.key().ownerId())).isEqualTo(5);
            keys.revoke(operator,second.key().id());
            assertThat(request(secondPort,second.token(),HttpMethod.GET,"/api/v1/auth/me",null,null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        }
    }
    @Test void administrativeHttpValidationAndOneTimeIssuanceWork() {
        var created=request(OPERATOR_TOKEN,HttpMethod.POST,"/api/v1/admin/owners",Map.of("name","HTTP owner","submissionLimitPerMinute",2));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String owner=created.getBody().get("id").asText();
        var issued=request(OPERATOR_TOKEN,HttpMethod.POST,"/api/v1/admin/owners/"+owner+"/keys",Map.of("name","HTTP key"));
        assertThat(issued.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(issued.getHeaders().getCacheControl()).contains("no-store");
        assertThat(request(issued.getBody().get("token").asText(),HttpMethod.GET,"/api/v1/auth/me",null).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(request(OPERATOR_TOKEN,HttpMethod.POST,"/api/v1/admin/owners/"+owner+"/limit",Map.of("submissionLimitPerMinute",0)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(request(OPERATOR_TOKEN,HttpMethod.POST,"/api/v1/admin/owners/"+owner+"/keys",Map.of("name","","role","ADMIN")).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(request(OPERATOR_TOKEN,HttpMethod.GET,"/api/v1/admin/audit?size=101",null).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(request(OPERATOR_TOKEN,HttpMethod.GET,"/api/v1/admin/owners?page=-1",null).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(request(OPERATOR_TOKEN,HttpMethod.GET,"/api/v1/admin/owners/"+UUID.randomUUID(),null).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
