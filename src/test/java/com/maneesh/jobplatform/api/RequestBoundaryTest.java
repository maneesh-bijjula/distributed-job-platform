package com.maneesh.jobplatform.api;

import com.maneesh.jobplatform.auth.ApiKeyService;
import com.maneesh.jobplatform.config.SecurityConfiguration;
import com.maneesh.jobplatform.jobs.JobService;
import com.maneesh.jobplatform.jobs.Job;
import com.maneesh.jobplatform.jobs.JobType;
import com.maneesh.jobplatform.jobs.JobStatus;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.security.test.context.support.WithAnonymousUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest({JobController.class, AccessController.class})
@Import(SecurityConfiguration.class)
@WithMockUser(username="00000000-0000-0000-0000-000000000001", roles="OPERATOR")
class RequestBoundaryTest {
    @Autowired MockMvc mvc;
    @MockitoBean JobService jobs;
    @MockitoBean ApiKeyService keys;

    @Test
    void oversizedJsonIsRejectedBeforeParsingOrSubmission() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content(" ".repeat(131073)))
                .andExpect(status().isPayloadTooLarge())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.status").value(413));
        verifyNoInteractions(jobs, keys);
    }

    @ParameterizedTest
    @ValueSource(strings={
            "{\"type\":\"SHA256\",\"text\":\"one\",\"text\":\"two\"}",
            "{\"type\":\"SHA256\",\"text\":\"one\"} {\"type\":\"UPPERCASE\",\"text\":\"two\"}",
            "{\"type\":\"SHA256\",\"text\":\"\\ud800\"}",
            "{\"type\":\"SHA256\",\"text\":\"\\udc00\"}"})
    void ambiguousOrInvalidUnicodeJsonCannotCreateAJob(String body) throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(jobs, keys);
    }

    @ParameterizedTest
    @ValueSource(strings={"\\ud800", "\\udc00", "x\\ud800y"})
    void invalidUnicodeNamesCannotReachStorage(String name) throws Exception {
        mvc.perform(post("/api/v1/admin/owners").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\""+name+"\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(jobs, keys);
    }

    @Test
    @WithAnonymousUser
    void authenticationRunsBeforeBodyBuffering() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(" ".repeat(131073)))
                .andExpect(status().isUnauthorized());
        verifyNoInteractions(jobs, keys);
    }

    @Test
    @WithMockUser(roles="METRICS")
    void authorizationRunsBeforeBodyBuffering() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON).content(" ".repeat(131073)))
                .andExpect(status().isForbidden());
        verifyNoInteractions(jobs, keys);
    }

    @Test
    void supplementaryUnicodeAndTrailingWhitespaceAreAccepted() throws Exception {
        var job = new Job(UUID.randomUUID(), JobType.SHA256, "hello 😀", JobStatus.QUEUED,
                null, null, Instant.now(), null, 0, null, null);
        when(jobs.submitFor(any(), any(), eq("hello 😀"), isNull(), eq(0), isNull(), isNull(), isNull()))
                .thenReturn(new JobService.Submission(job, true));
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"SHA256\",\"text\":\"hello \\ud83d\\ude00\"}\n  "))
                .andExpect(status().isAccepted());
    }
}
