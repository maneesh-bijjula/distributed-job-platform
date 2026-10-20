package com.maneesh.jobplatform.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.EnumMap;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class OperationMetrics {
    public enum Operation { OUTBOX_DISPATCH, EXECUTION_RECOVERY, SNAPSHOT_REFRESH, BROKER_PROBE }
    private final EnumMap<Operation,Counter> failures=new EnumMap<>(Operation.class);
    public OperationMetrics(ObjectProvider<MeterRegistry> provider) {
        var registry=provider.getIfAvailable(SimpleMeterRegistry::new);
        for(var operation:Operation.values()) failures.put(operation,Counter.builder("job.platform.operation.failures")
                .description("Process-local failed operational passes; counters reset on process restart")
                .tag("operation",operation.name().toLowerCase(java.util.Locale.ROOT)).register(registry));
    }
    public void failed(Operation operation) { failures.get(operation).increment(); }
}
