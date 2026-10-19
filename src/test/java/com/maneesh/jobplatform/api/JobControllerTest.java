package com.maneesh.jobplatform.api;

import com.maneesh.jobplatform.jobs.*;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(JobController.class)
class JobControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean JobService service;

    @ParameterizedTest
    @ValueSource(strings = {
            "{}", "{\"type\":\"NOPE\",\"text\":\"hello\"}",
            "{\"type\":\"SHA256\",\"text\":\"  \"}",
            "{\"type\":\"SHA256\",\"text\":null}",
            "{\"type\":\"SHA256\",\"text\":\"ok\",\"unexpected\":true}", "not-json",
            "{\"type\":1,\"text\":\"hello\"}",
            "{\"type\":\"SHA256\",\"text\":\"hello\",\"priority\":-1}",
            "{\"type\":\"SHA256\",\"text\":\"hello\",\"priority\":10}",
            "{\"type\":\"SHA256\",\"text\":\"hello\",\"priority\":1.5}",
            "{\"type\":\"SHA256\",\"text\":\"hello\",\"scheduledAt\":\"not-a-date\"}",
            "{\"type\":\"SHA256\",\"text\":\"hello\",\"scheduledAt\":\"+10000-01-01T00:00:00Z\"}"
    })
    void rejectsInvalidBodiesBeforeSubmission(String body) throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void rejectsOversizedText() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"SHA256\",\"text\":\"" + "x".repeat(16001) + "\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void rejectsNullCharacterBeforeItReachesPostgres() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"SHA256\",\"text\":\"hello\\u0000world\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @ParameterizedTest
    @ValueSource(strings = {"?page=-1", "?size=0", "?size=101", "?page=1000001", "?status=UNKNOWN", "?page=nope"})
    void rejectsUnboundedOrInvalidPagination(String query) throws Exception {
        mvc.perform(get("/api/v1/jobs" + query)).andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void rejectsInvalidIdempotencyKey() throws Exception {
        mvc.perform(post("/api/v1/jobs").header("Idempotency-Key", "invalid key!")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"type\":\"SHA256\",\"text\":\"abc\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void returnsAcceptedWithLocation() throws Exception {
        UUID id = UUID.randomUUID();
        var job = new Job(id, JobType.SHA256, "abc", JobStatus.QUEUED, null, null, Instant.now(), null, 0, null, null);
        when(service.submit(JobType.SHA256, "abc", "request-1", 0, null, null)).thenReturn(new JobService.Submission(job, true));
        mvc.perform(post("/api/v1/jobs").header("Idempotency-Key", "request-1")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"type\":\"SHA256\",\"text\":\"abc\"}"))
                .andExpect(status().isAccepted()).andExpect(header().string("Location", "/api/v1/jobs/" + id))
                .andExpect(jsonPath("$.id").value(id.toString())).andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @Test
    void fullFinalPageDoesNotClaimAnotherPageExists() throws Exception {
        var job = new Job(UUID.randomUUID(), JobType.SHA256, "abc", JobStatus.QUEUED, null, null, Instant.now(), null, 0, null, null);
        when(service.list(null, 0, 1)).thenReturn(List.of(job));
        mvc.perform(get("/api/v1/jobs?size=1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.hasNext").value(false));
    }

    @Test
    void cancellationReturnsPersistedTerminalState() throws Exception {
        UUID id = UUID.randomUUID();
        when(service.cancel(id)).thenReturn(new Job(id, JobType.SHA256, "abc", JobStatus.CANCELLED,
                null, null, Instant.now(), Instant.now(), 0, null, null));
        mvc.perform(post("/api/v1/jobs/" + id + "/cancel"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("CANCELLED"));
        verify(service).cancel(id);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void retryReturnsNewOrReplayedJobWithLinkAndLocation(boolean created) throws Exception {
        UUID source = UUID.randomUUID();
        UUID child = UUID.randomUUID();
        Job job = new Job(child, JobType.SHA256, "abc", JobStatus.QUEUED, null, null,
                Instant.now(), null, 7, null, source);
        when(service.retry(source, "retry-1")).thenReturn(new JobService.Submission(job, created));
        mvc.perform(post("/api/v1/jobs/" + source + "/retry").header("Idempotency-Key", "retry-1"))
                .andExpect(status().is(created ? 202 : 200))
                .andExpect(header().string("Location", "/api/v1/jobs/" + child))
                .andExpect(jsonPath("$.retryOf").value(source.toString()))
                .andExpect(jsonPath("$.priority").value(7));
    }

    @Test
    void retryRequiresAnIdempotencyKey() throws Exception {
        mvc.perform(post("/api/v1/jobs/" + UUID.randomUUID() + "/retry"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "bad key!"})
    void retryRejectsInvalidIdempotencyKey(String key) throws Exception {
        mvc.perform(post("/api/v1/jobs/" + UUID.randomUUID() + "/retry").header("Idempotency-Key", key))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @ParameterizedTest
    @ValueSource(strings = {"cancel", "retry"})
    void controlActionsRejectUnexpectedRequestBodies(String action) throws Exception {
        mvc.perform(post("/api/v1/jobs/" + UUID.randomUUID() + "/" + action)
                        .header("Idempotency-Key", "valid-key").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"priority\":9}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @ParameterizedTest
    @ValueSource(strings = {"?page=-1", "?page=1000001", "?size=0", "?size=101", "?page=nope"})
    void eventHistoryRejectsInvalidPagination(String query) throws Exception {
        mvc.perform(get("/api/v1/jobs/" + UUID.randomUUID() + "/events" + query))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void eventHistoryPagesHaveAnAccurateContinuationFlag() throws Exception {
        UUID id = UUID.randomUUID();
        var first = new JobEvent(1L, id, JobEvent.Type.SUBMITTED, JobStatus.QUEUED, null, Instant.now());
        var second = new JobEvent(2L, id, JobEvent.Type.CANCELLED, JobStatus.CANCELLED, null, Instant.now());
        when(service.events(id, 0, 1)).thenReturn(List.of(first, second));
        mvc.perform(get("/api/v1/jobs/" + id + "/events?size=1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].type").value("SUBMITTED"))
                .andExpect(jsonPath("$.hasNext").value(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "3601", "1.5"})
    void rejectsInvalidExecutionTimeout(String timeout) throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"type\":\"SHA256\",\"text\":\"abc\",\"timeoutSeconds\":" + timeout + "}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void explicitExecutionTimeoutIsPassedToSubmission() throws Exception {
        UUID id = UUID.randomUUID();
        var job = new Job(id, JobType.SHA256, "abc", JobStatus.QUEUED, null, null,
                Instant.now(), null, 0, null, null, 7, 0, null, null, 0);
        when(service.submit(JobType.SHA256, "abc", null, 0, null, 7)).thenReturn(new JobService.Submission(job, true));
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"type\":\"SHA256\",\"text\":\"abc\",\"timeoutSeconds\":7}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.timeoutSeconds").value(7));
    }

    @ParameterizedTest
    @ValueSource(strings = {"?page=-1", "?size=101", "?size=0", "?page=1000001", "?page=nope"})
    void attemptHistoryRejectsInvalidPagination(String query) throws Exception {
        mvc.perform(get("/api/v1/jobs/" + UUID.randomUUID() + "/attempts" + query))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void attemptResponseContainsOperationalDetailsWithoutClaimSecrets() throws Exception {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        var attempt = new JobAttempt(UUID.randomUUID(), id, 1, 0, "worker-test", JobAttempt.Status.RUNNING,
                now, now, now.plusSeconds(3), now.plusSeconds(30), null, null);
        when(service.attempts(id, 0, 1)).thenReturn(List.of(attempt));
        mvc.perform(get("/api/v1/jobs/" + id + "/attempts?size=1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.hasNext").value(false))
                .andExpect(jsonPath("$.items[0].status").value("RUNNING"))
                .andExpect(jsonPath("$.items[0].leaseUntil").exists())
                .andExpect(jsonPath("$.items[0].claimToken").doesNotExist());
    }
}
