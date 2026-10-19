package com.maneesh.jobplatform.config;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class ExecutionConfiguration {
    @Bean
    ThreadPoolTaskScheduler taskScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("job-dispatch-");
        return scheduler;
    }
    @Bean(destroyMethod = "shutdownNow")
    ScheduledThreadPoolExecutor executionMonitor(ExecutionProperties properties) {
        var monitor = new ScheduledThreadPoolExecutor(properties.monitorThreads(), runnable -> {
            var thread = new Thread(runnable, "job-lease-monitor");
            thread.setDaemon(true);
            return thread;
        });
        monitor.setRemoveOnCancelPolicy(true);
        return monitor;
    }
}
