package com.maneesh.jobplatform.integration;

import org.springframework.jdbc.core.JdbcTemplate;

final class IntegrationDatabase {
    private IntegrationDatabase() {}

    static void requireTestDatabase(JdbcTemplate jdbc) {
        String database = jdbc.queryForObject("SELECT current_database()", String.class);
        if (database == null || !database.endsWith("_test")) {
            throw new IllegalStateException("Integration tests require a database name ending in _test");
        }
    }

    static void reset(JdbcTemplate jdbc) {
        requireTestDatabase(jdbc);
        jdbc.execute("TRUNCATE job_events, job_outbox, jobs");
    }
}
