package com.maneesh.jobplatform.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.dao.DataAccessException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

public class ApiKeyFilter extends OncePerRequestFilter {
    private final ApiKeyService service;
    private final ObjectMapper mapper;
    public ApiKeyFilter(ApiKeyService service, ObjectMapper mapper) { this.service=service; this.mapper=mapper; }

    @Override protected boolean shouldNotFilter(HttpServletRequest request) {
        return request.getServletPath().equals("/actuator/health");
    }
    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        var headers = Collections.list(request.getHeaders("Authorization"));
        if (headers.isEmpty()) { chain.doFilter(request,response); return; }
        String header = headers.getFirst();
        if (headers.size() != 1 || header.length() != 54 || !header.regionMatches(true,0,"Bearer ",0,7)) {
            write(mapper,response,401,"A valid bearer API key is required"); return;
        }
        java.util.Optional<ApiPrincipal> principal;
        try { principal = service.authenticate(header.substring(7)); }
        catch (DataAccessException error) {
            write(mapper,response,503,"Authentication storage is temporarily unavailable"); return;
        }
        if (principal.isEmpty()) { write(mapper,response,401,"A valid bearer API key is required"); return; }
        var context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(new UsernamePasswordAuthenticationToken(principal.get(),null,
                List.of(new SimpleGrantedAuthority("ROLE_" + principal.get().role().name()))));
        SecurityContextHolder.setContext(context);
        try { chain.doFilter(request,response); }
        finally { SecurityContextHolder.clearContext(); }
    }

    public static void write(ObjectMapper mapper, HttpServletResponse response, int status, String detail) throws IOException {
        SecurityContextHolder.clearContext();
        response.setStatus(status);
        response.setContentType("application/problem+json");
        response.setHeader("Cache-Control","no-store");
        if (status == 401) response.setHeader("WWW-Authenticate","Bearer");
        mapper.writeValue(response.getOutputStream(),Map.of("type","about:blank","status",status,"detail",detail));
    }
}
