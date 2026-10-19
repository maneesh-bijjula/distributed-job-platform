package com.maneesh.jobplatform.auth;

import java.time.Instant;
import java.util.UUID;

public final class AccessRecords {
    private AccessRecords() {}
    public record Owner(UUID id, String name, int submissionLimitPerMinute, Instant createdAt) {}
    public record Key(UUID id, UUID ownerId, String name, ApiPrincipal.Role role, String prefix,
                      Instant createdAt, Instant revokedAt) {}
    public record IssuedKey(Key key, String token) {
        @Override public String toString() { return "IssuedKey[key=" + key.id() + ", token=REDACTED]"; }
    }
    public record Audit(long id, UUID actorKeyId, String action, UUID ownerId, UUID keyId, Instant occurredAt) {}
}
