package com.maneesh.jobplatform.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.jobs.JobType;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

@Component
public class JobProcessor {
    private static final Pattern WORD = Pattern.compile("\\S+", Pattern.UNICODE_CHARACTER_CLASS);
    private final ObjectMapper mapper;

    public JobProcessor(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public JsonNode process(JobType type, String text, ExecutionControl control) {
        control.checkpoint();
        JsonNode result = process(type, text);
        control.checkpoint();
        return result;
    }

    public JsonNode process(JobType type, String text) {
        var result = mapper.createObjectNode();
        return switch (type) {
            case WORD_COUNT -> result.put("wordCount", WORD.matcher(text).results().count())
                    .put("characterCount", text.codePointCount(0, text.length()));
            case UPPERCASE -> result.put("text", text.toUpperCase(Locale.ROOT));
            case SHA256 -> result.put("sha256", sha256(text));
        };
    }

    private String sha256(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("JVM does not provide SHA-256", exception);
        }
    }
}
