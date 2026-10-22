package com.maneesh.jobplatform.integration;

import com.maneesh.jobplatform.auth.*;
import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.observability.*;
import com.maneesh.jobplatform.worker.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT,properties={
        "jobs.dispatcher.enabled=false","jobs.worker.enabled=false","jobs.metrics.refresh-ms=60000"})
@DirtiesContext(classMode=DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureObservability
@ExtendWith(OutputCaptureExtension.class)
class ObservabilityIT extends AuthenticatedIntegrationTest {
    @Autowired JobService jobs;
    @Autowired JobExecutor executor;
    @Autowired ExecutionService execution;
    @Autowired JdbcTemplate jdbc;
    @Autowired JobMetricsExporter exporter;
    @Autowired MeterRegistry registry;
    @MockitoSpyBean MetricsSnapshotRepository snapshots;
    @MockitoSpyBean JobProcessor processor;
    @Autowired OperationMetrics operations;
    @Autowired RabbitTemplate rabbit;
    @Autowired ApiKeyService keys;
    @Autowired PlatformTransactionManager transactions;
    @LocalServerPort int port;

    @BeforeEach void reset() { IntegrationDatabase.reset(jdbc); }
    private Job submit() { return jobs.submit(JobType.UPPERCASE,"private observability payload",null).job(); }
    private double count(String event) { return snapshots.load().value("event:"+event+":UPPERCASE"); }
    private MetricsSnapshotRepository.Timing time(String outcome) { return snapshots.load().timing("EXECUTION:UPPERCASE:"+outcome); }

    @Test void oneLockedMetricsStripeDoesNotBlockAnUnrelatedJobsCancellation() throws Exception {
        UUID first=new UUID(0,1),second=new UUID(0,2);
        for(UUID id:new UUID[]{first,second})
            jdbc.update("INSERT INTO jobs(id,type,input_text) VALUES (?,'UPPERCASE','stripe isolation')",id);
        jobs.cancel(first);
        var locked=new CountDownLatch(1);
        var release=new CountDownLatch(1);
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()) {
            var holder=pool.submit(()->new TransactionTemplate(transactions).executeWithoutResult(status->{
                jdbc.queryForObject("SELECT value FROM job_metric_counts WHERE event='CANCELLED' AND job_type='UPPERCASE' AND stripe=? FOR UPDATE",Long.class,first.hashCode() & 31);
                locked.countDown();
                try { if(!release.await(10,TimeUnit.SECONDS)) throw new IllegalStateException("Stripe test timed out"); }
                catch(InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
            }));
            try {
                assertThat(locked.await(5,TimeUnit.SECONDS)).isTrue();
                assertThat(pool.submit(()->jobs.cancel(second)).get(5,TimeUnit.SECONDS).status()).isEqualTo(JobStatus.CANCELLED);
            } finally { release.countDown(); }
            holder.get(5,TimeUnit.SECONDS);
        }
        assertThat(count("CANCELLED")).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_metric_counts WHERE event='CANCELLED'",Integer.class)).isEqualTo(2);
    }

    @Test void legacyStripeAndNewStripesExportOneBoundedTotal() {
        jdbc.update("INSERT INTO job_metric_counts(event,job_type,value) VALUES ('SUBMITTED','UPPERCASE',70)");
        jdbc.update("INSERT INTO job_metric_counts(event,job_type,stripe,value) VALUES ('SUBMITTED','UPPERCASE',31,30)");
        jdbc.update("INSERT INTO job_metric_timings(kind,job_type,outcome,sample_count,total_seconds) VALUES ('EXECUTION','UPPERCASE','SUCCEEDED',7,3.5)");
        jdbc.update("INSERT INTO job_metric_timings(kind,job_type,outcome,stripe,sample_count,total_seconds) VALUES ('EXECUTION','UPPERCASE','SUCCEEDED',31,3,1.5)");
        assertThat(count("SUBMITTED")).isEqualTo(100);
        assertThat(time("SUCCEEDED")).isEqualTo(new MetricsSnapshotRepository.Timing(10,5));
        assertThatThrownBy(()->jdbc.update("INSERT INTO job_metric_counts(event,job_type,stripe,value) VALUES ('STARTED','UPPERCASE',32,1)"))
                .isInstanceOf(DataAccessException.class);
        exporter.refresh();
        assertThat(registry.get("job.platform.jobs.accepted").tag("type","UPPERCASE").functionCounter().count()).isEqualTo(100);
        assertThat(registry.getMeters().stream().filter(m->m.getId().getName().startsWith("job.platform."))
                .flatMap(m->m.getId().getTags().stream()).map(t->t.getKey())).doesNotContain("stripe");
    }

    @Test void successfulWorkAndDuplicateReplaysProduceOneCommittedCountAndTiming() {
        Job first=jobs.submit(JobType.UPPERCASE,"hello","metrics-replay").job();
        jobs.submit(JobType.UPPERCASE,"hello","metrics-replay");
        executor.execute(first.id()); executor.execute(first.id());
        assertThat(count("SUBMITTED")).isEqualTo(1);
        assertThat(count("STARTED")).isEqualTo(1);
        assertThat(count("SUCCEEDED")).isEqualTo(1);
        assertThat(time("SUCCEEDED").count()).isEqualTo(1);
        assertThat(time("SUCCEEDED").seconds()).isGreaterThanOrEqualTo(0);
        assertThat(snapshots.load().timing("QUEUE_DELAY:UPPERCASE:STARTED").count()).isEqualTo(1);
        exporter.refresh();
        assertThat(registry.get("job.platform.jobs.accepted").tag("type","UPPERCASE").functionCounter().count()).isEqualTo(1);
    }

    @Test void permanentFailureAndBothCancellationModesHaveAccurateAttemptOutcomes() {
        Job failed=submit(); execution.fail(execution.claim(failed.id(),0).orElseThrow());
        Job queued=submit(); jobs.cancel(queued.id()); jobs.cancel(queued.id());
        Job running=submit(); execution.claim(running.id(),0); jobs.cancel(running.id());
        assertThat(count("FAILED")).isEqualTo(1); assertThat(count("CANCELLED")).isEqualTo(2);
        assertThat(time("FAILED").count()).isEqualTo(1);
        assertThat(time("CANCELLED").count()).isEqualTo(1);
        jobs.retry(failed.id(),"manual-observability");
        assertThat(count("RETRY_REQUESTED")).isEqualTo(1);
    }

    @Test void retryableAttemptFailureIsNotCountedAsATerminalJobFailure() {
        Job job=jobs.submit(JobType.UPPERCASE,"retry",null,0,null,60,new RetryPolicy(2,20,20)).job();
        execution.fail(execution.claim(job.id(),0).orElseThrow(),HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        assertThat(count("RETRY_SCHEDULED")).isEqualTo(1);
        assertThat(count("FAILED")).isZero(); assertThat(time("FAILED").count()).isEqualTo(1);
        var snapshot=snapshots.load();
        assertThat(snapshot.value("retry:waiting")).isEqualTo(1);
        assertThat(snapshot.value("outbox:scheduled")).isEqualTo(1);
        jdbc.update("UPDATE jobs SET retry_at=clock_timestamp()-interval '200 milliseconds',eligible_at=clock_timestamp()-interval '200 milliseconds',created_at=clock_timestamp()-interval '1 hour' WHERE id=?",job.id());
        var second=execution.claim(job.id(),1).orElseThrow();
        assertThat(jdbc.queryForObject("SELECT queue_delay_seconds FROM job_attempts WHERE id=?",Double.class,second.attempt().id())).isBetween(0.15,3.0);
        execution.fail(second,HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        exporter.refresh();
        assertThat(registry.get("job.platform.jobs.failed").tag("type","UPPERCASE").functionCounter().count()).isEqualTo(1);
        assertThat(count("RETRIES_EXHAUSTED")).isEqualTo(1);
    }

    @Test void futureSchedulingIsExcludedFromDueBacklogAndQueueDelay() {
        Job future=jobs.submit(JobType.UPPERCASE,"future",null,0,Instant.now().plusSeconds(3600)).job();
        Job now=submit();
        assertThat(snapshots.load().value("outbox:scheduled")).isEqualTo(1);
        assertThat(snapshots.load().value("outbox:due")).isEqualTo(1);
        jdbc.update("UPDATE jobs SET created_at=clock_timestamp()-interval '2 hours',scheduled_at=clock_timestamp()-interval '1 hour',eligible_at=clock_timestamp()-interval '200 milliseconds' WHERE id=?",future.id());
        var claim=execution.claim(future.id(),0).orElseThrow();
        assertThat(jdbc.queryForObject("SELECT queue_delay_seconds FROM job_attempts WHERE id=?",Double.class,claim.attempt().id())).isBetween(0.15,3.0);
        jobs.cancel(now.id());
    }

    @Test void leaseLossRecoveryAndExhaustionAreVisibleWithoutWorkerIdLabels() {
        Job job=submit(); execution.claim(job.id(),0);
        jdbc.update("UPDATE job_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=?",job.id());
        assertThat(snapshots.load().value("expired_leases")).isEqualTo(1);
        execution.recoverBatch();
        assertThat(count("LEASE_EXPIRED")).isEqualTo(1); assertThat(time("ABANDONED").count()).isEqualTo(1);
        assertThat(snapshots.load().value("expired_leases")).isZero();
        Job capped=jobs.submit(JobType.UPPERCASE,"one attempt",null,0,null,30,new RetryPolicy(1,1,1)).job();
        execution.claim(capped.id(),0);
        jdbc.update("UPDATE job_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=?",capped.id());
        execution.recoverBatch();
        assertThat(count("RECOVERY_EXHAUSTED")).isEqualTo(1);
        assertThat(snapshots.load().value("dead:OPEN")).isEqualTo(1);
    }

    @Test void executionTimeoutContributesOneFailedJobAndOneTimedOutAttempt() {
        Job job=submit(); var claim=execution.claim(job.id(),0).orElseThrow();
        jdbc.update("UPDATE job_attempts SET deadline_at=clock_timestamp()-interval '1 second',lease_until=clock_timestamp()-interval '2 seconds' WHERE id=?",claim.attempt().id());
        jdbc.update("UPDATE jobs SET deadline_at=clock_timestamp()-interval '1 second' WHERE id=?",job.id());
        execution.recoverBatch(); execution.recoverBatch();
        assertThat(count("TIMED_OUT")).isEqualTo(1); assertThat(time("TIMED_OUT").count()).isEqualTo(1);
        exporter.refresh();
        assertThat(registry.get("job.platform.jobs.failed").tag("type","UPPERCASE").functionCounter().count()).isEqualTo(1);
    }

    @Test void beforeCommitMetricWriteFailureRollsBackJobAuditAndAdmission(CapturedOutput output) {
        jdbc.execute("CREATE FUNCTION reject_metric_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'metric storage unavailable'; END $$");
        jdbc.execute("CREATE TRIGGER reject_metric_write BEFORE INSERT ON job_metric_counts FOR EACH ROW EXECUTE FUNCTION reject_metric_write()");
        try {
            assertThatThrownBy(this::submit).isInstanceOf(DataAccessException.class);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs",Integer.class)).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM job_events",Integer.class)).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM job_outbox",Integer.class)).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM submission_usage",Integer.class)).isZero();
            assertThat(count("SUBMITTED")).isZero();
            assertThat(output.getOut()).doesNotContain("\"transition\":\"SUBMITTED\"");
        } finally {
            jdbc.execute("DROP TRIGGER reject_metric_write ON job_metric_counts"); jdbc.execute("DROP FUNCTION reject_metric_write()");
        }
        submit(); assertThat(count("SUBMITTED")).isEqualTo(1); // Transaction-local buffers were cleaned after rollback.
    }

    @Test void rollbackOnlyAndNestedTransactionsDoNotLeakDeferredSamples() {
        var outer=new TransactionTemplate(transactions);
        outer.executeWithoutResult(status->{ submit(); status.setRollbackOnly(); });
        assertThat(count("SUBMITTED")).isZero();
        var operator=keys.authenticate(OPERATOR_TOKEN).orElseThrow();
        UUID owner=keys.createOwner(operator,"Nested metrics",120).id();
        outer.executeWithoutResult(status->{
            submit();
            var inner=new TransactionTemplate(transactions);
            inner.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            inner.executeWithoutResult(ignored->jobs.submitFor(owner,JobType.UPPERCASE,"committed inner job",null,0,null,30,null));
            status.setRollbackOnly();
        });
        assertThat(count("SUBMITTED")).isEqualTo(1);
        submit(); assertThat(count("SUBMITTED")).isEqualTo(2);
    }

    @Test void exporterRefreshAndRecreationDoNotResetOrDoubleCountDurableTotals() {
        executor.execute(submit().id()); exporter.refresh(); exporter.refresh();
        var secondRegistry=new SimpleMeterRegistry();
        try {
            var second=new JobMetricsExporter(snapshots,rabbit,operations,secondRegistry);
            second.refresh();
            assertThat(secondRegistry.get("job.platform.jobs.succeeded").tag("type","UPPERCASE").functionCounter().count()).isEqualTo(1);
        } finally { secondRegistry.close(); }
        assertThat(count("SUCCEEDED")).isEqualTo(1);
    }

    @Test void failedSnapshotRetainsTotalsAndExposesStalenessWithoutLoggingPrivateErrors(CapturedOutput output) {
        submit(); exporter.refresh();
        doThrow(new DataAccessResourceFailureException("private snapshot SQL credential")).when(snapshots).load();
        exporter.refresh();
        assertThat(registry.get("job.platform.snapshot.up").gauge().value()).isZero();
        assertThat(registry.get("job.platform.jobs.accepted").tag("type","UPPERCASE").functionCounter().count()).isEqualTo(1);
        assertThat(registry.get("job.platform.snapshot.age").gauge().value()).isGreaterThanOrEqualTo(0);
        assertThat(output.getOut()).doesNotContain("private snapshot SQL credential");
    }

    @Test void structuredJobLogsCorrelateAttemptsWithoutInputsKeysOrRawExceptions(CapturedOutput output) {
        Job job=submit();
        doThrow(new RetryableJobException("private handler password and input")).when(processor).process(any(),any(),any());
        executor.execute(job.id());
        assertThat(output.getOut()).contains("\"event\":\"job_transition\"","\"job_id\":\""+job.id(),"\"attempt_id\":");
        assertThat(output.getOut()).doesNotContain("private observability payload","private handler password and input",OPERATOR_TOKEN);
    }

    @Test void metricLabelsAndScrapeKeyPermissionsAreBounded() {
        var operator=keys.authenticate(OPERATOR_TOKEN).orElseThrow();
        var issued=keys.issue(operator,operator.ownerId(),"Metrics test",ApiPrincipal.Role.METRICS);
        var client=new TestRestTemplate(); var headers=new HttpHeaders(); headers.setBearerAuth(issued.token());
        for(String path:new String[]{"/api/v1/jobs","/api/v1/admin/owners","/api/v1/dead-letters","/actuator/info"})
            assertThat(client.exchange("http://localhost:"+port+path,HttpMethod.GET,new HttpEntity<>(headers),String.class).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        Job job=submit(); executor.execute(job.id()); exporter.refresh();
        var response=client.exchange("http://localhost:"+port+"/actuator/prometheus",HttpMethod.GET,new HttpEntity<>(headers),String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("job_platform_jobs_accepted_total","job_platform_execution_duration_seconds_count","job_platform_queue_delay_seconds_sum");
        assertThat(response.getBody()).doesNotContain(job.id().toString(),"private observability payload",issued.token(),OPERATOR_TOKEN);
        Set<String> allowed=Set.of("type","outcome","mode","status","state","dependency","queue","operation");
        registry.getMeters().stream().filter(m->m.getId().getName().startsWith("job.platform.")).forEach(m->
                m.getId().getTags().forEach(tag->assertThat(allowed).contains(tag.getKey())));
        keys.revoke(operator,issued.key().id());
        assertThat(client.exchange("http://localhost:"+port+"/actuator/prometheus",HttpMethod.GET,new HttpEntity<>(headers),String.class).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test void mixedTypeBatchesCommitRollupsWithoutInverseMetricLocks() throws Exception {
        var operator=keys.authenticate(OPERATOR_TOKEN).orElseThrow();
        UUID a=keys.createOwner(operator,"Metrics batch A",120).id(),b=keys.createOwner(operator,"Metrics batch B",120).id();
        var barrier=new CountDownLatch(2);
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()) {
            var first=pool.submit(()->batch(a,JobType.SHA256,JobType.UPPERCASE,barrier));
            var second=pool.submit(()->batch(b,JobType.UPPERCASE,JobType.SHA256,barrier));
            first.get(8,TimeUnit.SECONDS); second.get(8,TimeUnit.SECONDS);
        }
        assertThat(count("SUBMITTED")).isEqualTo(2);
        assertThat(snapshots.load().value("event:SUBMITTED:SHA256")).isEqualTo(2);
    }
    private void batch(UUID owner,JobType first,JobType second,CountDownLatch barrier) {
        new TransactionTemplate(transactions).executeWithoutResult(status->{
            jobs.submitFor(owner,first,"bounded batch",null,0,null,30,null);
            barrier.countDown();
            try { if(!barrier.await(5,TimeUnit.SECONDS)) throw new IllegalStateException("Batch barrier timed out"); }
            catch(InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
            jobs.submitFor(owner,second,"bounded batch",null,0,null,30,null);
        });
    }
}
