package com.maneesh.jobplatform.integration;

import com.maneesh.jobplatform.auth.KeyTokens;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@Import(AuthenticatedIntegrationTest.Client.class)
abstract class AuthenticatedIntegrationTest {
    static final String OPERATOR_TOKEN = KeyTokens.generate();
    @DynamicPropertySource
    static void auth(DynamicPropertyRegistry registry) { registry.add("jobs.auth.bootstrap-key", () -> OPERATOR_TOKEN); }
    @TestConfiguration(proxyBeanMethods=false)
    static class Client {
        @Bean RestTemplateBuilder restTemplateBuilder() {
            return new RestTemplateBuilder().defaultHeader("Authorization","Bearer "+OPERATOR_TOKEN);
        }
    }
}
