package com.maneesh.jobplatform.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.deadletter.*;
import com.maneesh.jobplatform.auth.ApiPrincipal;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/v1/dead-letters")
public class DeadLetterController {
    private final DeadLetterService service;
    private final DeadLetterCollector collector;
    public DeadLetterController(DeadLetterService service, DeadLetterCollector collector) {
        this.service = service; this.collector = collector;
    }
    public record Page(List<DeadLetter> items, int page, int size, boolean hasNext) {}

    @GetMapping
    public Page list(@RequestParam(required = false) DeadLetter.State state,
                     @RequestParam(defaultValue = "0") @Min(0) @Max(1000000) int page,
                     @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        var items = service.list(state, page, size);
        return new Page(items.subList(0, Math.min(size, items.size())), page, size, items.size() > size);
    }

    @GetMapping("/{id}")
    public DeadLetter get(@PathVariable UUID id) { return service.get(id); }

    @PostMapping("/collect")
    public DeadLetterCollector.Collection collect(@RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit,
                                                 @RequestBody(required = false) JsonNode body) {
        empty(body);
        return collector.collect(limit);
    }

    @PostMapping("/{id}/recover")
    public ResponseEntity<DeadLetter> recover(@AuthenticationPrincipal ApiPrincipal actor, @PathVariable UUID id,
            @RequestHeader("Idempotency-Key") @Pattern(regexp = "[A-Za-z0-9._:-]{1,128}") String key,
            @RequestBody(required = false) JsonNode body) {
        empty(body);
        var result = service.recoverAs(actor,id, key);
        return ResponseEntity.status(result.created() ? 202 : 200)
                .location(URI.create("/api/v1/jobs/" + result.entry().recoveryJobId())).body(result.entry());
    }

    @PostMapping("/{id}/discard")
    public DeadLetter discard(@AuthenticationPrincipal ApiPrincipal actor, @PathVariable UUID id,
            @RequestHeader("Idempotency-Key") @Pattern(regexp = "[A-Za-z0-9._:-]{1,128}") String key,
            @RequestBody(required = false) JsonNode body) {
        empty(body);
        return service.discardAs(actor,id, key).entry();
    }

    private void empty(JsonNode body) {
        if (body != null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "This action accepts no request body");
    }
}
