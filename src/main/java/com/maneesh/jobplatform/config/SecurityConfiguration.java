package com.maneesh.jobplatform.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.auth.*;
import jakarta.servlet.DispatcherType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class SecurityConfiguration {
    @Bean
    SecurityFilterChain apiSecurity(HttpSecurity http, ApiKeyService keys, ObjectMapper mapper) throws Exception {
        return http.csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .requestCache(cache -> cache.disable())
                .authorizeHttpRequests(auth -> auth
                        .dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                        .requestMatchers("/actuator/health").permitAll()
                        .requestMatchers("/api/v1/admin/**","/api/v1/dead-letters/**","/actuator/prometheus","/actuator/info").hasRole("OPERATOR")
                        .requestMatchers("/api/v1/jobs","/api/v1/jobs/**","/api/v1/auth/me").authenticated()
                        .anyRequest().denyAll())
                .exceptionHandling(errors -> errors
                        .authenticationEntryPoint((req,res,error) -> ApiKeyFilter.write(mapper,res,401,"A valid bearer API key is required"))
                        .accessDeniedHandler((req,res,error) -> ApiKeyFilter.write(mapper,res,403,"This key does not have permission for this operation")))
                .addFilterBefore(new ApiKeyFilter(keys,mapper), UsernamePasswordAuthenticationFilter.class)
                .build();
    }
}
