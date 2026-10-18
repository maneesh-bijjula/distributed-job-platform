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
            "{\"type\":\"SHA256\",\"text\":\"ok\",\"unexpected\":true}", "not-json"
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
        var job = new Job(id, JobType.SHA256, "abc", JobStatus.QUEUED, null, null, Instant.now(), null);
        when(service.submit(JobType.SHA256, "abc", "request-1")).thenReturn(new JobService.Submission(job, true));
        mvc.perform(post("/api/v1/jobs").header("Idempotency-Key", "request-1")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"type\":\"SHA256\",\"text\":\"abc\"}"))
                .andExpect(status().isAccepted()).andExpect(header().string("Location", "/api/v1/jobs/" + id))
                .andExpect(jsonPath("$.id").value(id.toString())).andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @Test
    void fullFinalPageDoesNotClaimAnotherPageExists() throws Exception {
        var job = new Job(UUID.randomUUID(), JobType.SHA256, "abc", JobStatus.QUEUED, null, null, Instant.now(), null);
        when(service.list(null, 0, 1)).thenReturn(List.of(job));
        mvc.perform(get("/api/v1/jobs?size=1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.hasNext").value(false));
    }
}
