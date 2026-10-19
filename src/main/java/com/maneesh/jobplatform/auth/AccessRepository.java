package com.maneesh.jobplatform.auth;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import static com.maneesh.jobplatform.auth.AccessRecords.*;

@Repository
public class AccessRepository {
    private final NamedParameterJdbcTemplate jdbc;
    public AccessRepository(NamedParameterJdbcTemplate jdbc) { this.jdbc = jdbc; }

    public Optional<ApiPrincipal> authenticate(String hash) {
        return jdbc.query("SELECT id,owner_id,role FROM api_keys WHERE token_hash=:hash AND revoked_at IS NULL", Map.of("hash", hash),
                (rs,n) -> new ApiPrincipal(rs.getObject("owner_id", UUID.class), rs.getObject("id", UUID.class), ApiPrincipal.Role.valueOf(rs.getString("role"))))
                .stream().findFirst();
    }

    public boolean insertKey(UUID id, UUID owner, String name, ApiPrincipal.Role role, String token, UUID actor) {
        var params = new HashMap<String,Object>();
        params.put("id", id); params.put("owner", owner); params.put("name", name); params.put("role", role.name());
        params.put("hash", KeyTokens.hash(token)); params.put("prefix", token.substring(0,12)); params.put("actor", actor);
        return jdbc.update("""
                INSERT INTO api_keys(id,owner_id,name,role,token_hash,prefix,created_by)
                VALUES (:id,:owner,:name,:role,:hash,:prefix,:actor) ON CONFLICT(token_hash) DO NOTHING
                """, params) == 1;
    }
    public Key key(UUID id) {
        return jdbc.query("SELECT * FROM api_keys WHERE id=:id", Map.of("id", id), this::mapKey).stream().findFirst().orElse(null);
    }
    public Optional<Owner> owner(UUID id) {
        return jdbc.query("SELECT * FROM owners WHERE id=:id", Map.of("id", id), this::mapOwner).stream().findFirst();
    }
    public List<Owner> owners(int page, int size) {
        return jdbc.query("SELECT * FROM owners ORDER BY created_at,id LIMIT :size OFFSET :offset", Map.of("size", size+1, "offset", (long)page*size), this::mapOwner);
    }
    public void createOwner(UUID id, String name, int limit) {
        jdbc.update("INSERT INTO owners(id,name,submission_limit_per_minute) VALUES (:id,:name,:limit)", Map.of("id", id, "name", name, "limit", limit));
    }
    public void limit(UUID id, int limit) {
        jdbc.update("UPDATE owners SET submission_limit_per_minute=:limit WHERE id=:id", Map.of("id", id, "limit", limit));
    }
    public List<Key> keys(UUID owner, int page, int size) {
        return jdbc.query("SELECT * FROM api_keys WHERE owner_id=:owner ORDER BY created_at,id LIMIT :size OFFSET :offset",
                Map.of("owner", owner, "size", size+1, "offset", (long)page*size), this::mapKey);
    }
    public boolean revoke(UUID id) {
        return jdbc.update("UPDATE api_keys SET revoked_at=clock_timestamp() WHERE id=:id AND revoked_at IS NULL", Map.of("id", id)) == 1;
    }
    public int activeOperators() {
        return jdbc.queryForObject("SELECT count(*) FROM api_keys WHERE role='OPERATOR' AND revoked_at IS NULL", Map.of(), Integer.class);
    }
    public void audit(UUID actor, String action, UUID owner, UUID key) {
        var params = new HashMap<String,Object>();
        params.put("actor", actor); params.put("action", action); params.put("owner", owner); params.put("key", key);
        jdbc.update("INSERT INTO access_audit(actor_key_id,action,owner_id,key_id) VALUES (:actor,:action,:owner,:key)", params);
    }
    public List<Audit> audit(int page, int size) {
        return jdbc.query("SELECT * FROM access_audit ORDER BY id LIMIT :size OFFSET :offset", Map.of("size", size+1, "offset", (long)page*size),
                (rs,n) -> new Audit(rs.getLong("id"), rs.getObject("actor_key_id", UUID.class), rs.getString("action"),
                        rs.getObject("owner_id", UUID.class), rs.getObject("key_id", UUID.class), rs.getTimestamp("occurred_at").toInstant()));
    }
    private Key mapKey(ResultSet rs, int row) throws SQLException {
        var revoked = rs.getTimestamp("revoked_at");
        return new Key(rs.getObject("id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("name"),
                ApiPrincipal.Role.valueOf(rs.getString("role")), rs.getString("prefix"), rs.getTimestamp("created_at").toInstant(),
                revoked == null ? null : revoked.toInstant());
    }
    private Owner mapOwner(ResultSet rs, int row) throws SQLException {
        return new Owner(rs.getObject("id", UUID.class), rs.getString("name"), rs.getInt("submission_limit_per_minute"), rs.getTimestamp("created_at").toInstant());
    }
}
