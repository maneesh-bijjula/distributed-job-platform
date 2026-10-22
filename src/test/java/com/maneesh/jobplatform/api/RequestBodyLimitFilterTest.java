package com.maneesh.jobplatform.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class RequestBodyLimitFilterTest {
    private final RequestBodyLimitFilter filter = new RequestBodyLimitFilter(new ObjectMapper());

    private MockHttpServletRequest unknownLength(byte[] bytes) {
        var request = new MockHttpServletRequest() {
            @Override public int getContentLength() { return -1; }
            @Override public long getContentLengthLong() { return -1; }
        };
        request.setMethod("POST");
        request.setContent(bytes);
        return request;
    }

    @Test
    void unknownLengthOverLimitNeverReachesDownstream() throws Exception {
        var response = new MockHttpServletResponse();
        filter.doFilter(unknownLength(new byte[RequestBodyLimitFilter.MAX_BYTES + 1]), response,
                (request, result) -> fail("Oversized streaming input reached MVC"));
        assertThat(response.getStatus()).isEqualTo(413);
        assertThat(response.getHeader("Cache-Control")).isEqualTo("no-store");
    }

    @Test
    void knownOversizeDoesNotReadBody() throws Exception {
        var request = new MockHttpServletRequest() {
            @Override public long getContentLengthLong() { return RequestBodyLimitFilter.MAX_BYTES + 1L; }
            @Override public jakarta.servlet.ServletInputStream getInputStream() {
                throw new AssertionError("Known oversized input must not be read");
            }
        };
        request.setMethod("POST");
        var response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> fail("Oversized input reached MVC"));
        assertThat(response.getStatus()).isEqualTo(413);
    }

    @Test
    void exactLimitPassesAndEveryByteRemainsAvailable() throws Exception {
        byte[] bytes = "é".repeat(RequestBodyLimitFilter.MAX_BYTES / 2).getBytes(StandardCharsets.UTF_8);
        filter.doFilter(unknownLength(bytes), new MockHttpServletResponse(), (request, response) -> {
            assertThat(request.getContentLengthLong()).isEqualTo(bytes.length);
            var stream = request.getInputStream();
            assertThat(stream.isFinished()).isFalse();
            assertThat(stream.readAllBytes()).isEqualTo(bytes);
            assertThat(stream.isFinished()).isTrue();
        });
    }

    @Test
    void byteLimitIncludesMultibyteUtf8() throws Exception {
        var response = new MockHttpServletResponse();
        filter.doFilter(unknownLength("é".repeat(65537).getBytes(StandardCharsets.UTF_8)), response,
                (request, result) -> fail("Multibyte input bypassed byte limit"));
        assertThat(response.getStatus()).isEqualTo(413);
    }

    @Test
    void clientReadFailurePropagatesWithoutCallingApplication() {
        var request = new MockHttpServletRequest() {
            @Override public jakarta.servlet.ServletInputStream getInputStream() {
                return new jakarta.servlet.ServletInputStream() {
                    @Override public int read() throws IOException { throw new IOException("client disconnected"); }
                    @Override public boolean isFinished() { return false; }
                    @Override public boolean isReady() { return true; }
                    @Override public void setReadListener(jakarta.servlet.ReadListener listener) {}
                };
            }
        };
        request.setMethod("POST");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> filter.doFilter(request, new MockHttpServletResponse(),
                (req, res) -> fail("Incomplete request reached MVC"))).isInstanceOf(IOException.class);
    }
}
