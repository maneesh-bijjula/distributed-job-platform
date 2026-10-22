package com.maneesh.jobplatform.auth;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import static com.maneesh.jobplatform.auth.AccessRecords.*;
import com.maneesh.jobplatform.validation.WellFormedUnicode;

@Service
public class ApiKeyService {
    private final AccessRepository repository;
    public ApiKeyService(AccessRepository repository) { this.repository = repository; }

    public Optional<ApiPrincipal> authenticate(String token) {
        return KeyTokens.valid(token) ? repository.authenticate(KeyTokens.hash(token)) : Optional.empty();
    }

    @Transactional
    public void bootstrap(String token) {
        if (token != null && !token.isBlank()) {
            if (!KeyTokens.valid(token)) throw new IllegalStateException("JOB_BOOTSTRAP_API_KEY has an invalid format; use scripts/init-auth.py");
            UUID id = UUID.randomUUID();
            if (repository.insertKey(id, ApiPrincipal.LEGACY_OWNER, "Bootstrap operator", ApiPrincipal.Role.OPERATOR, token, null)) {
                repository.audit(null, "BOOTSTRAP_KEY_CREATED", ApiPrincipal.LEGACY_OWNER, id);
            }
        }
        if (repository.activeOperators() == 0) {
            throw new IllegalStateException("No active operator key; set a fresh JOB_BOOTSTRAP_API_KEY using scripts/init-auth.py");
        }
    }

    public List<Owner> owners(ApiPrincipal actor, int page, int size) { operator(actor); return repository.owners(page,size); }
    public Owner owner(UUID id) {
        return repository.owner(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,"Owner not found"));
    }
    @Transactional
    public Owner createOwner(ApiPrincipal actor, String name, Integer limit) {
        operator(actor); validName(name); validLimit(limit == null ? 120 : limit);
        UUID id = UUID.randomUUID();
        repository.createOwner(id,name,limit == null ? 120 : limit);
        repository.audit(actor.keyId(),"OWNER_CREATED",id,null);
        return owner(id);
    }
    @Transactional
    public Owner limit(ApiPrincipal actor, UUID id, int limit) {
        operator(actor); owner(id); validLimit(limit);
        repository.limit(id,limit);
        repository.audit(actor.keyId(),"LIMIT_CHANGED",id,null);
        return owner(id);
    }
    @Transactional
    public IssuedKey issue(ApiPrincipal actor, UUID owner, String name, ApiPrincipal.Role role) {
        operator(actor); owner(owner); validName(name);
        if (role == null) role = ApiPrincipal.Role.USER;
        UUID id = UUID.randomUUID();
        String token = KeyTokens.generate();
        if (!repository.insertKey(id,owner,name,role,token,actor.keyId())) throw new IllegalStateException("Cannot allocate an API key");
        repository.audit(actor.keyId(),"KEY_CREATED",owner,id);
        return new IssuedKey(repository.key(id),token);
    }
    public List<Key> keys(ApiPrincipal actor, UUID owner, int page, int size) {
        operator(actor); owner(owner); return repository.keys(owner,page,size);
    }
    @Transactional
    public Key revoke(ApiPrincipal actor, UUID id) {
        operator(actor);
        Key key = repository.key(id);
        if (key == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"API key not found");
        if (repository.revoke(id)) repository.audit(actor.keyId(),"KEY_REVOKED",key.ownerId(),id);
        return repository.key(id);
    }
    public List<Audit> audit(ApiPrincipal actor, int page, int size) { operator(actor); return repository.audit(page,size); }
    private void operator(ApiPrincipal actor) {
        if (actor == null || actor.role() != ApiPrincipal.Role.OPERATOR) throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Operator access required");
    }
    private void validName(String name) {
        if (name == null || name.isBlank() || name.length() > 80 || name.indexOf('\0') >= 0
                || !WellFormedUnicode.Validator.valid(name))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Name must be nonblank, contain no null characters, and have at most 80 characters");
    }
    private void validLimit(int limit) {
        if (limit < 1 || limit > 10000) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Submission limit must be between 1 and 10000");
    }
}
