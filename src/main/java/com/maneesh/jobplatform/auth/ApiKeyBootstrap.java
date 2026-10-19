package com.maneesh.jobplatform.auth;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class ApiKeyBootstrap implements ApplicationRunner {
    private final ApiKeyService service;
    private final String token;
    public ApiKeyBootstrap(ApiKeyService service, @Value("${jobs.auth.bootstrap-key:}") String token) { this.service=service; this.token=token; }
    @Override public void run(ApplicationArguments args) { service.bootstrap(token); }
}
