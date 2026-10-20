package com.maneesh.jobplatform.auth;

import java.security.Principal;
import java.util.UUID;

public record ApiPrincipal(UUID ownerId, UUID keyId, Role role) implements Principal {
    public static final UUID LEGACY_OWNER = UUID.fromString("00000000-0000-0000-0000-000000000001");
    public enum Role { USER, OPERATOR, METRICS }
    @Override public String getName() { return ownerId.toString(); }
}
