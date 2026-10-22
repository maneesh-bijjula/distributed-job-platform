package com.maneesh.jobplatform.api;

import com.maneesh.jobplatform.auth.ApiKeyService;
import com.maneesh.jobplatform.config.SecurityConfiguration;
import com.maneesh.jobplatform.jobs.JobService;
import com.maneesh.jobplatform.jobs.Job;
import com.maneesh.jobplatform.jobs.JobType;
import com.maneesh.jobplatform.jobs.JobStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(BatchJobController.class)
@Import(SecurityConfiguration.class)
@WithMockUser(username="00000000-0000-0000-0000-000000000001",roles="USER")
class BatchJobControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean JobService jobs;
    @MockitoBean ApiKeyService keys;

    @ParameterizedTest
    @ValueSource(strings={"{}","{\"items\":[]}","{\"items\":[null]}","{\"items\":[{}]}",
            "{\"items\":[{\"idempotencyKey\":\"bad key!\",\"job\":{\"type\":\"SHA256\",\"text\":\"ok\"}}]}",
            "{\"items\":[{\"idempotencyKey\":\"valid\",\"job\":{\"type\":\"SHA256\",\"text\":\"\\ud800\"}}]}",
            "{\"items\":[{\"idempotencyKey\":\"valid\",\"job\":{\"type\":\"SHA256\",\"text\":\"ok\",\"ownerId\":\"forged\"}}]}"})
    void invalidBatchIsRejectedBeforeAnyItemIsSubmitted(String body) throws Exception {
        mvc.perform(post("/api/v1/jobs/batch").contentType(MediaType.APPLICATION_JSON).content(body)).andExpect(status().isBadRequest());
        verifyNoInteractions(jobs,keys);
    }

    @ParameterizedTest
    @ValueSource(ints={2,101})
    void duplicateKeysAndOversizedBatchesNeverStartSubmission(int count) throws Exception {
        String items=IntStream.range(0,count).mapToObj(i->"{\"idempotencyKey\":\""+(count==2?"duplicate":i)+"\",\"job\":{\"type\":\"SHA256\",\"text\":\"ok\"}}")
                .collect(Collectors.joining(","));
        mvc.perform(post("/api/v1/jobs/batch").contentType(MediaType.APPLICATION_JSON).content("{\"items\":["+items+"]}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(jobs,keys);
    }

    @Test
    void exhaustedCooperativeBudgetDoesNotStartRemainingTransactions() {
        var time=new AtomicLong();
        var job=new Job(UUID.randomUUID(),JobType.UPPERCASE,"one",JobStatus.QUEUED,null,null,Instant.now(),null,0,null,null);
        when(jobs.submitFor(any(),any(),anyString(),anyString(),anyInt(),isNull(),isNull(),isNull())).thenAnswer(call->{
            time.set(6_000_000_000L);return new JobService.Submission(job,true);
        });
        var controller=new BatchJobController(jobs,time::get);
        var request=new CreateJobRequest(JobType.UPPERCASE,"one",0,null,null,null);
        var response=controller.submit(()->"00000000-0000-0000-0000-000000000001",new BatchJobRequest(List.of(
                new BatchJobRequest.Item("first",request),new BatchJobRequest.Item("second",request))));
        assertThat(response.getStatusCode().value()).isEqualTo(207);
        assertThat(response.getBody().created()).isEqualTo(1);
        assertThat(response.getBody().unsuccessful()).isEqualTo(1);
        assertThat(response.getBody().items().get(1).status()).isEqualTo(503);
        verify(jobs,times(1)).submitFor(any(),any(),anyString(),anyString(),anyInt(),isNull(),isNull(),isNull());
    }

    @Test
    @WithMockUser(roles="METRICS")
    void metricsKeysCannotSubmitBatches() throws Exception {
        mvc.perform(post("/api/v1/jobs/batch").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isForbidden());
        verifyNoInteractions(jobs,keys);
    }
}
