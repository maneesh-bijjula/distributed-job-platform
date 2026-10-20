package com.maneesh.jobplatform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.maneesh.jobplatform.config.ExecutionProperties;
import com.maneesh.jobplatform.config.MetricsProperties;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;

@EnableScheduling
@EnableConfigurationProperties({ExecutionProperties.class,MetricsProperties.class})
@SpringBootApplication(exclude = UserDetailsServiceAutoConfiguration.class)
public class JobPlatformApplication {
    public static void main(String[] args) {
        SpringApplication.run(JobPlatformApplication.class, args);
    }
}
