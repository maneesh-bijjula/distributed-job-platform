package com.maneesh.jobplatform.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.auth.*;
import com.maneesh.jobplatform.validation.WellFormedUnicode;
import com.maneesh.jobplatform.auth.AccessRecords.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/v1")
public class AccessController {
    private final ApiKeyService service;
    public AccessController(ApiKeyService service) { this.service=service; }
    public record OwnerRequest(@NotBlank @Size(max=80) @WellFormedUnicode @Pattern(regexp="[^\\x00]*") String name,
                               @Min(1) @Max(10000) Integer submissionLimitPerMinute) {}
    public record KeyRequest(@NotBlank @Size(max=80) @WellFormedUnicode @Pattern(regexp="[^\\x00]*") String name, ApiPrincipal.Role role) {}
    public record LimitRequest(@Min(1) @Max(10000) int submissionLimitPerMinute) {}
    public record Page<T>(List<T> items, int page, int size, boolean hasNext) {}
    public record Me(ApiPrincipal identity, Owner owner) {}

    @GetMapping("/auth/me")
    public Me me(@AuthenticationPrincipal ApiPrincipal actor) { return new Me(actor,service.owner(actor.ownerId())); }

    @PostMapping("/admin/owners")
    public ResponseEntity<Owner> create(@AuthenticationPrincipal ApiPrincipal actor, @Valid @RequestBody OwnerRequest request) {
        Owner owner=service.createOwner(actor,request.name(),request.submissionLimitPerMinute());
        return ResponseEntity.created(URI.create("/api/v1/admin/owners/"+owner.id())).body(owner);
    }
    @GetMapping("/admin/owners")
    public Page<Owner> owners(@AuthenticationPrincipal ApiPrincipal actor,
            @RequestParam(defaultValue="0") @Min(0) @Max(1000000) int page,
            @RequestParam(defaultValue="20") @Min(1) @Max(100) int size) { return page(service.owners(actor,page,size),page,size); }
    @GetMapping("/admin/owners/{id}")
    public Owner owner(@PathVariable UUID id) { return service.owner(id); }
    @PostMapping("/admin/owners/{id}/limit")
    public Owner limit(@AuthenticationPrincipal ApiPrincipal actor, @PathVariable UUID id, @Valid @RequestBody LimitRequest request) {
        return service.limit(actor,id,request.submissionLimitPerMinute());
    }
    @PostMapping("/admin/owners/{id}/keys")
    public ResponseEntity<IssuedKey> issue(@AuthenticationPrincipal ApiPrincipal actor, @PathVariable UUID id,
                                          @Valid @RequestBody KeyRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).header("Cache-Control","no-store")
                .body(service.issue(actor,id,request.name(),request.role()));
    }
    @GetMapping("/admin/owners/{id}/keys")
    public Page<Key> keys(@AuthenticationPrincipal ApiPrincipal actor, @PathVariable UUID id,
            @RequestParam(defaultValue="0") @Min(0) @Max(1000000) int page,
            @RequestParam(defaultValue="20") @Min(1) @Max(100) int size) { return page(service.keys(actor,id,page,size),page,size); }
    @PostMapping("/admin/keys/{id}/revoke")
    public Key revoke(@AuthenticationPrincipal ApiPrincipal actor, @PathVariable UUID id, @RequestBody(required=false) JsonNode body) {
        if(body!=null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"This action accepts no request body");
        return service.revoke(actor,id);
    }
    @GetMapping("/admin/audit")
    public Page<Audit> audit(@AuthenticationPrincipal ApiPrincipal actor,
            @RequestParam(defaultValue="0") @Min(0) @Max(1000000) int page,
            @RequestParam(defaultValue="20") @Min(1) @Max(100) int size) { return page(service.audit(actor,page,size),page,size); }
    private <T> Page<T> page(List<T> items,int page,int size) {
        return new Page<>(items.subList(0,Math.min(size,items.size())),page,size,items.size()>size);
    }
}
