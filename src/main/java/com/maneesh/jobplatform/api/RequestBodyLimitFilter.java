package com.maneesh.jobplatform.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.auth.ApiKeyFilter;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.springframework.web.filter.OncePerRequestFilter;

/** Bounds bytes before JSON decoding, including requests without Content-Length. */
public final class RequestBodyLimitFilter extends OncePerRequestFilter {
    public static final int MAX_BYTES = 128 * 1024;
    private final ObjectMapper mapper;

    public RequestBodyLimitFilter(ObjectMapper mapper) { this.mapper = mapper; }

    @Override protected boolean shouldNotFilter(HttpServletRequest request) {
        return !Set.of("POST", "PUT", "PATCH", "DELETE").contains(request.getMethod());
    }

    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                             FilterChain chain) throws ServletException, IOException {
        if (request.getContentLengthLong() > MAX_BYTES) {
            reject(response);
            return;
        }
        byte[] body = request.getInputStream().readNBytes(MAX_BYTES + 1);
        if (body.length > MAX_BYTES) {
            reject(response);
            return;
        }
        // The synchronous MVC endpoints need one bounded, replayable stream after this read.
        chain.doFilter(new HttpServletRequestWrapper(request) {
            private final ByteArrayInputStream input = new ByteArrayInputStream(body);
            private final ServletInputStream stream = new ServletInputStream() {
                @Override public int read() { return input.read(); }
                @Override public int read(byte[] bytes, int offset, int length) { return input.read(bytes, offset, length); }
                @Override public boolean isFinished() { return input.available() == 0; }
                @Override public boolean isReady() { return true; }
                @Override public void setReadListener(ReadListener listener) {
                    throw new IllegalStateException("This endpoint uses synchronous request processing");
                }
            };
            @Override public ServletInputStream getInputStream() { return stream; }
            @Override public BufferedReader getReader() {
                Charset charset = getCharacterEncoding() == null ? StandardCharsets.UTF_8 : Charset.forName(getCharacterEncoding());
                return new BufferedReader(new InputStreamReader(stream, charset));
            }
            @Override public int getContentLength() { return body.length; }
            @Override public long getContentLengthLong() { return body.length; }
        }, response);
    }

    private void reject(HttpServletResponse response) throws IOException {
        ApiKeyFilter.write(mapper, response, 413, "Request body must not exceed 131072 bytes");
    }
}
