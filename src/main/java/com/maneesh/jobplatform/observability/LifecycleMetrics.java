package com.maneesh.jobplatform.observability;

import com.maneesh.jobplatform.jobs.*;
import java.util.Map;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Bounded rollups are part of the lifecycle transaction; logs describe committed transitions only. */
@Repository
public class LifecycleMetrics {
    private static final Logger log=LoggerFactory.getLogger(LifecycleMetrics.class);
    private final NamedParameterJdbcTemplate jdbc;
    public LifecycleMetrics(NamedParameterJdbcTemplate jdbc) { this.jdbc=jdbc; }

    public record EventSample(JobType type,Double queueSeconds,String attemptOutcome,Double executionSeconds) {}

    @Transactional(propagation=Propagation.MANDATORY)
    public void record(UUID jobId, JobEvent.Type event, JobStatus status, UUID attemptId, EventSample sample) {
        JobType type=sample.type();
        int stripe=jobId.hashCode() & 31;
        Pending pending=pending();
        pending.counts.merge(event.name()+":"+type.name()+":"+stripe,1L,Long::sum);
        if(attemptId!=null) {
            if(event==JobEvent.Type.STARTED) {
                if(sample.queueSeconds()!=null) timing(pending,"QUEUE_DELAY",type,"STARTED",stripe,sample.queueSeconds());
            } else if(event==JobEvent.Type.SUCCEEDED || event==JobEvent.Type.FAILED || event==JobEvent.Type.CANCELLED
                    || event==JobEvent.Type.RETRY_SCHEDULED || event==JobEvent.Type.RETRIES_EXHAUSTED
                    || event==JobEvent.Type.LEASE_EXPIRED || event==JobEvent.Type.TIMED_OUT || event==JobEvent.Type.RECOVERY_EXHAUSTED) {
                if(sample.executionSeconds()!=null)
                    timing(pending,"EXECUTION",type,sample.attemptOutcome(),stripe,sample.executionSeconds());
            }
        }
        pending.logs.add(() -> {
                var entry=log.atInfo().addKeyValue("event","job_transition").addKeyValue("job_id",jobId)
                        .addKeyValue("job_type",type.name()).addKeyValue("transition",event.name()).addKeyValue("job_status",status.name());
                if(attemptId!=null) entry.addKeyValue("attempt_id",attemptId);
                entry.log("Job lifecycle transition committed");
        });
    }
    private void timing(Pending pending,String kind,JobType type,String outcome,int stripe,double seconds) {
        pending.timings.merge(kind+":"+type.name()+":"+outcome+":"+stripe,new MetricsSnapshotRepository.Timing(1,seconds),
                (a,b)->new MetricsSnapshotRepository.Timing(a.count()+b.count(),a.seconds()+b.seconds()));
    }
    private Pending pending() {
        var existing=(Pending)TransactionSynchronizationManager.getResource(this);
        if(existing!=null) return existing;
        var pending=new Pending();
        TransactionSynchronizationManager.bindResource(this,pending);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void beforeCommit(boolean readOnly) { LifecycleMetrics.this.flush(pending); }
            @Override public void afterCommit() { pending.logs.forEach(Runnable::run); }
            @Override public void suspend() { TransactionSynchronizationManager.unbindResource(LifecycleMetrics.this); }
            @Override public void resume() { TransactionSynchronizationManager.bindResource(LifecycleMetrics.this,pending); }
            @Override public void afterCompletion(int status) {
                if(TransactionSynchronizationManager.getResource(LifecycleMetrics.this)==pending)
                    TransactionSynchronizationManager.unbindResource(LifecycleMetrics.this);
            }
        });
        return pending;
    }
    private void flush(Pending pending) {
        // A recovery batch may touch different job types. Every transaction locks rollup rows
        // in the same order, and only after lifecycle writes, avoiding inverse metric-row locks.
        pending.counts.forEach((key,count)-> {
            String[] parts=key.split(":");
            jdbc.update("""
                    INSERT INTO job_metric_counts(event,job_type,stripe,value) VALUES (:event,:type,:stripe,:count)
                    ON CONFLICT(event,job_type,stripe) DO UPDATE SET value=job_metric_counts.value+EXCLUDED.value
                    """,Map.of("event",parts[0],"type",parts[1],"stripe",Integer.parseInt(parts[2]),"count",count));
        });
        pending.timings.forEach((key,sample)-> {
            String[] parts=key.split(":");
            jdbc.update("""
                    INSERT INTO job_metric_timings(kind,job_type,outcome,stripe,sample_count,total_seconds)
                    VALUES (:kind,:type,:outcome,:stripe,:count,:seconds)
                    ON CONFLICT(kind,job_type,outcome,stripe) DO UPDATE SET sample_count=job_metric_timings.sample_count+EXCLUDED.sample_count,
                        total_seconds=job_metric_timings.total_seconds+EXCLUDED.total_seconds
                    """,Map.of("kind",parts[0],"type",parts[1],"outcome",parts[2],"stripe",Integer.parseInt(parts[3]),"count",sample.count(),"seconds",sample.seconds()));
        });
    }
    private static final class Pending {
        final TreeMap<String,Long> counts=new TreeMap<>();
        final TreeMap<String,MetricsSnapshotRepository.Timing> timings=new TreeMap<>();
        final ArrayList<Runnable> logs=new ArrayList<>();
    }
}
