package com.maneesh.jobplatform.integration;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class IntegrationDatabaseTest {
    @Test
    void applicationDatabaseIsRejectedBeforeAnyTablesAreCleared() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject("SELECT current_database()", String.class)).thenReturn("jobs");
        assertThatThrownBy(() -> IntegrationDatabase.reset(jdbc))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("_test");
        verify(jdbc, never()).execute(anyString());
    }

    @Test
    void explicitlyNamedTestDatabaseCanBeReset() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject("SELECT current_database()", String.class)).thenReturn("jobs_test");
        IntegrationDatabase.reset(jdbc);
        verify(jdbc).execute("TRUNCATE job_events, job_attempts, job_outbox, jobs");
    }
}
