package loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.JobPlatformApplication;
import com.maneesh.jobplatform.jobs.JobType;
import com.maneesh.jobplatform.worker.ExecutionControl;
import com.maneesh.jobplatform.worker.JobProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

/** Fault-test executable only; neither this class nor its delay hook is in the application image. */
@Configuration(proxyBeanMethods=false)
public class ControlledWorker {
    public static void main(String[] args) {
        SpringApplication.run(new Class<?>[]{JobPlatformApplication.class,ControlledWorker.class},args);
    }

    @Bean @Primary
    JobProcessor controlledProcessor(ObjectMapper mapper,Environment environment) {
        long delay=environment.getRequiredProperty("LOAD_HANDLER_DELAY_MS",Long.class);
        if(delay<50 || delay>5000) throw new IllegalArgumentException("Fixture delay must be 50–5000 ms");
        return new JobProcessor(mapper) {
            @Override public JsonNode process(JobType type,String text,ExecutionControl control) {
                control.checkpoint();
                // Deliberately non-cooperative: resuming a frozen worker must reach the durable final-write guard.
                try { Thread.sleep(delay); }
                catch(InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException("Fixture interrupted"); }
                return super.process(type,text);
            }
        };
    }
}
