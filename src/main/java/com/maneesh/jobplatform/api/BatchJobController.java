package com.maneesh.jobplatform.api;

import com.maneesh.jobplatform.auth.AdmissionService;
import com.maneesh.jobplatform.jobs.Job;
import com.maneesh.jobplatform.jobs.JobService;
import jakarta.validation.Valid;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.TransactionException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Each service call commits independently; a batch never holds 100 jobs in one transaction. */
@RestController
@RequestMapping("/api/v1/jobs/batch")
public class BatchJobController {
    private static final Logger log=LoggerFactory.getLogger(BatchJobController.class);
    private final JobService jobs;
    private final LongSupplier clock;
    private final Map<Integer,Counter> outcomes=new HashMap<>();
    @Autowired
    public BatchJobController(JobService jobs,ObjectProvider<MeterRegistry> registries) {
        this(jobs,System::nanoTime,registries.getIfAvailable(SimpleMeterRegistry::new));
    }
    BatchJobController(JobService jobs,LongSupplier clock) { this(jobs,clock,new SimpleMeterRegistry()); }
    private BatchJobController(JobService jobs,LongSupplier clock,MeterRegistry registry) {
        this.jobs=jobs;this.clock=clock;
        for(int status:new int[]{200,202,400,409,429,500,503,0})
            outcomes.put(status,Counter.builder("job.platform.batch.items").tag("status",status==0?"other":Integer.toString(status))
                    .description("Process-local batch item outcomes; committed admission and idempotency replay are separate statuses").register(registry));
    }
    public record Item(String idempotencyKey,int status,Job job,ProblemDetail error,Integer retryAfterSeconds) {}
    public record Result(List<Item> items,int created,int replayed,int unsuccessful) {}

    @PostMapping
    public ResponseEntity<Result> submit(Principal principal,@Valid @RequestBody BatchJobRequest request) {
        UUID owner=UUID.fromString(principal.getName());
        var items=new ArrayList<Item>(request.items().size());
        int created=0,replayed=0,unsuccessful=0;
        long started=clock.getAsLong();
        for(var item:request.items()) {
            // Stop starting further work after the cooperative budget; an in-flight transaction
            // finishes through the normal service boundary and its key resolves any ambiguity.
            if(clock.getAsLong()-started>=TimeUnit.SECONDS.toNanos(5)) {
                unsuccessful++;
                items.add(failure(item.idempotencyKey(),503,"Batch processing budget exceeded; retry this item with the same key",null));
                continue;
            }
            try {
                var job=item.job();
                var result=jobs.submitFor(owner,job.type(),job.text(),item.idempotencyKey(),job.priority(),
                        job.scheduledAt(),job.timeoutSeconds(),job.retryPolicy());
                items.add(record(new Item(item.idempotencyKey(),result.created()?202:200,result.job(),null,null)));
                if(result.created()) created++; else replayed++;
            } catch(AdmissionService.Limited error) {
                unsuccessful++;
                items.add(failure(item.idempotencyKey(),429,"Owner submission limit exceeded",error.retryAfter()));
            } catch(ResponseStatusException error) {
                unsuccessful++;
                int status=error.getStatusCode().value();
                items.add(failure(item.idempotencyKey(),status,status<500?error.getReason():"Job submission failed",null));
            } catch(DataAccessException | TransactionException error) {
                unsuccessful++;
                items.add(failure(item.idempotencyKey(),503,"Job storage is temporarily unavailable",null));
                log.atWarn().addKeyValue("event","batch_item_storage_failed").addKeyValue("error_kind","STORAGE_UNAVAILABLE")
                        .log("Batch item admission could not be confirmed; retry with the same key");
            } catch(RuntimeException error) {
                unsuccessful++;
                items.add(failure(item.idempotencyKey(),500,"Job submission failed",null));
                log.atError().addKeyValue("event","batch_item_failed").addKeyValue("error_kind","UNEXPECTED")
                        .log("Batch item admission could not be confirmed; retry with the same key");
            }
        }
        return ResponseEntity.status(unsuccessful>0?207:created>0?202:200)
                .body(new Result(List.copyOf(items),created,replayed,unsuccessful));
    }

    private Item failure(String key,int status,String detail,Integer retryAfter) {
        return record(new Item(key,status,null,ProblemDetail.forStatusAndDetail(org.springframework.http.HttpStatusCode.valueOf(status),detail),retryAfter));
    }
    private Item record(Item item) {
        outcomes.getOrDefault(item.status(),outcomes.get(0)).increment();
        return item;
    }
}
