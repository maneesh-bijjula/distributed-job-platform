package com.maneesh.jobplatform.observability;

import com.maneesh.jobplatform.config.RabbitConfiguration;
import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.deadletter.DeadLetter;
import io.micrometer.core.instrument.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** One API endpoint exports global database rollups, including work performed by non-web workers. */
@Component
@ConditionalOnWebApplication(type=ConditionalOnWebApplication.Type.SERVLET)
public class JobMetricsExporter {
    private static final Logger log=LoggerFactory.getLogger(JobMetricsExporter.class);
    private final MetricsSnapshotRepository repository;
    private final RabbitAdmin rabbit;
    private final OperationMetrics operations;
    private volatile MetricsSnapshotRepository.Snapshot snapshot=new MetricsSnapshotRepository.Snapshot(Map.of(),Map.of());
    private volatile boolean snapshotUp;
    private volatile double snapshotAt;
    private volatile Broker broker=new Broker(false,Double.NaN,Double.NaN,0);
    private record Broker(boolean up,double ready,double dead,double at) {}

    public JobMetricsExporter(MetricsSnapshotRepository repository,RabbitTemplate rabbit,OperationMetrics operations,MeterRegistry registry) {
        this.repository=repository; this.rabbit=new RabbitAdmin(rabbit.getConnectionFactory()); this.operations=operations;
        for(var type:JobType.values()) {
            counter(registry,"jobs.accepted",type,"SUBMITTED"); counter(registry,"jobs.started",type,"STARTED");
            counter(registry,"jobs.succeeded",type,"SUCCEEDED"); counter(registry,"jobs.cancelled",type,"CANCELLED");
            counter(registry,"jobs.failed",type,"FAILED","TIMED_OUT","RETRIES_EXHAUSTED","RECOVERY_EXHAUSTED");
            counter(registry,"jobs.timed.out",type,"TIMED_OUT"); counter(registry,"worker.recoveries",type,"LEASE_EXPIRED");
            counter(registry,"worker.recovery.exhausted",type,"RECOVERY_EXHAUSTED");
            counter(registry,"jobs.retry.exhausted",type,"RETRIES_EXHAUSTED");
            for(String mode:new String[]{"automatic","manual"}) {
                String event=mode.equals("automatic")?"RETRY_SCHEDULED":"RETRY_REQUESTED";
                FunctionCounter.builder("job.platform.jobs.retried",this,m->m.count(type,event))
                        .tags("type",type.name(),"mode",mode).description("Committed automatic retries or linked manual retry requests").register(registry);
            }
            FunctionTimer.builder("job.platform.queue.delay",this,m->m.timing("QUEUE_DELAY",type,"STARTED").count(),
                    m->m.timing("QUEUE_DELAY",type,"STARTED").seconds(),TimeUnit.SECONDS)
                    .tag("type",type.name()).description("Attempt queue wait after eligibility; excludes scheduling and backoff").register(registry);
            for(var outcome:JobAttempt.Status.values()) {
                if(outcome==JobAttempt.Status.RUNNING) continue;
                FunctionTimer.builder("job.platform.execution.duration",this,m->m.timing("EXECUTION",type,outcome.name()).count(),
                        m->m.timing("EXECUTION",type,outcome.name()).seconds(),TimeUnit.SECONDS)
                        .tags("type",type.name(),"outcome",outcome.name()).description("Committed attempt wall duration through logical completion").register(registry);
            }
            for(var status:JobStatus.values()) Gauge.builder("job.platform.jobs",this,m->m.snapshot.value("jobs:"+type.name()+":"+status.name()))
                    .tags("type",type.name(),"status",status.name()).description("Current global durable job state, including legacy jobs").register(registry);
        }
        for(String state:new String[]{"due","scheduled"}) Gauge.builder("job.platform.outbox.entries",this,m->m.snapshot.value("outbox:"+state))
                .tag("state",state).register(registry);
        for(String state:new String[]{"waiting","due"}) Gauge.builder("job.platform.retry.jobs",this,m->m.snapshot.value("retry:"+state))
                .tag("state",state).register(registry);
        for(var state:DeadLetter.State.values()) Gauge.builder("job.platform.dead.letters",this,m->m.snapshot.value("dead:"+state.name()))
                .tag("state",state.name()).register(registry);
        Gauge.builder("job.platform.outbox.oldest.due.age",this,m->m.snapshot.value("outbox:oldest")).baseUnit("seconds").register(registry);
        Gauge.builder("job.platform.queue.oldest.due.age",this,m->m.snapshot.value("retry:oldest")).baseUnit("seconds").register(registry);
        Gauge.builder("job.platform.expired.leases",this,m->m.snapshot.value("expired_leases")).register(registry);
        Gauge.builder("job.platform.snapshot.up",this,m->m.snapshotUp?1:0).register(registry);
        Gauge.builder("job.platform.snapshot.age",this,m->age(m.snapshotAt)).baseUnit("seconds").register(registry);
        Gauge.builder("job.platform.dependency.up",this,m->m.snapshotUp?1:0).tag("dependency","postgres").register(registry);
        Gauge.builder("job.platform.dependency.up",this,m->m.broker.up()?1:0).tag("dependency","rabbitmq").register(registry);
        Gauge.builder("job.platform.broker.messages",this,m->m.broker.ready()).tag("queue","ready").register(registry);
        Gauge.builder("job.platform.broker.messages",this,m->m.broker.dead()).tag("queue","dead").register(registry);
        Gauge.builder("job.platform.broker.snapshot.age",this,m->age(m.broker.at())).baseUnit("seconds").register(registry);
    }
    private void counter(MeterRegistry registry,String name,JobType type,String...events) {
        FunctionCounter.builder("job.platform."+name,this,m->m.count(type,events)).tag("type",type.name())
                .description("Committed durable lifecycle transitions since observability migration").register(registry);
    }
    private double count(JobType type,String...events) {
        double total=0; var current=snapshot;
        for(String event:events) total+=current.value("event:"+event+":"+type.name());
        return total;
    }
    private MetricsSnapshotRepository.Timing timing(String kind,JobType type,String outcome) {
        return snapshot.timing(kind+":"+type.name()+":"+outcome);
    }
    private static double age(double at) { return at==0?Double.NaN:Math.max(0,Instant.now().toEpochMilli()/1000.0-at); }

    @Scheduled(scheduler="observabilityScheduler",fixedDelayString="${jobs.metrics.refresh-ms:5000}",initialDelay=1000)
    public void refresh() {
        try { snapshot=repository.load(); snapshotAt=Instant.now().toEpochMilli()/1000.0; snapshotUp=true; }
        catch(RuntimeException error) {
            snapshotUp=false; operations.failed(OperationMetrics.Operation.SNAPSHOT_REFRESH);
            log.atWarn().addKeyValue("event","metrics_refresh_failed").addKeyValue("error_kind","STORAGE_UNAVAILABLE")
                    .log("Metrics retain their last successful snapshot; freshness exposes the failure");
        }
    }
    @Scheduled(scheduler="observabilityScheduler",fixedDelayString="${jobs.metrics.refresh-ms:5000}",initialDelay=1000)
    public void probeBroker() {
        try {
            var ready=rabbit.getQueueInfo(RabbitConfiguration.QUEUE);
            var legacy=rabbit.getQueueInfo(RabbitConfiguration.LEGACY_QUEUE);
            var dead=rabbit.getQueueInfo(RabbitConfiguration.DEAD_QUEUE);
            if(ready==null || legacy==null || dead==null) throw new IllegalStateException("Broker queues unavailable");
            broker=new Broker(true,ready.getMessageCount()+legacy.getMessageCount(),dead.getMessageCount(),Instant.now().toEpochMilli()/1000.0);
        } catch(RuntimeException error) {
            broker=new Broker(false,Double.NaN,Double.NaN,broker.at()); operations.failed(OperationMetrics.Operation.BROKER_PROBE);
            log.atWarn().addKeyValue("event","broker_probe_failed").addKeyValue("error_kind","BROKER_UNAVAILABLE")
                    .log("Broker depth is unavailable; durable outbox work remains in PostgreSQL");
        }
    }
}
