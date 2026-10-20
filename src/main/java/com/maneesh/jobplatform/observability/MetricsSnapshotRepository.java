package com.maneesh.jobplatform.observability;

import java.util.HashMap;
import java.util.Map;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class MetricsSnapshotRepository {
    private final NamedParameterJdbcTemplate jdbc;
    public MetricsSnapshotRepository(NamedParameterJdbcTemplate jdbc) { this.jdbc=jdbc; }
    public record Timing(long count,double seconds) {}
    public record Snapshot(Map<String,Double> values,Map<String,Timing> timings) {
        public double value(String key) { return values.getOrDefault(key,0.0); }
        public Timing timing(String key) { return timings.getOrDefault(key,new Timing(0,0)); }
    }
    @Transactional(readOnly=true,isolation=Isolation.REPEATABLE_READ,timeout=5)
    public Snapshot load() {
        var values=new HashMap<String,Double>();
        var timings=new HashMap<String,Timing>();
        jdbc.query("SELECT event,job_type,value FROM job_metric_counts",Map.of(),rs -> {
            values.put("event:"+rs.getString("event")+":"+rs.getString("job_type"),rs.getDouble("value"));
        });
        jdbc.query("SELECT * FROM job_metric_timings",Map.of(),rs -> {
            timings.put(rs.getString("kind")+":"+rs.getString("job_type")+":"+rs.getString("outcome"),
                    new Timing(rs.getLong("sample_count"),rs.getDouble("total_seconds")));
        });
        jdbc.query("SELECT type,status,count(*) AS count FROM jobs GROUP BY type,status",Map.of(),rs -> {
            values.put("jobs:"+rs.getString("type")+":"+rs.getString("status"),rs.getDouble("count"));
        });
        jdbc.query("SELECT state,count(*) AS count FROM dead_letters GROUP BY state",Map.of(),rs -> {
            values.put("dead:"+rs.getString("state"),rs.getDouble("count"));
        });
        var backlog=jdbc.queryForMap("""
                SELECT count(*) FILTER (WHERE available_at<=clock_timestamp()) AS due,
                    count(*) FILTER (WHERE available_at>clock_timestamp()) AS scheduled,
                    coalesce(max(greatest(0,extract(epoch FROM (clock_timestamp()-available_at))))
                        FILTER (WHERE available_at<=clock_timestamp()),0) AS oldest
                FROM job_outbox
                """,Map.of());
        backlog.forEach((key,value)->values.put("outbox:"+key,((Number)value).doubleValue()));
        var retries=jdbc.queryForMap("""
                SELECT count(*) FILTER (WHERE retry_at>clock_timestamp()) AS waiting,
                    count(*) FILTER (WHERE retry_at<=clock_timestamp()) AS due,
                    coalesce(max(greatest(0,extract(epoch FROM (clock_timestamp()-
                        coalesce(eligible_at,greatest(created_at,scheduled_at,retry_at)))))),0) AS oldest
                FROM jobs WHERE status='QUEUED'
                """,Map.of());
        retries.forEach((key,value)->values.put("retry:"+key,((Number)value).doubleValue()));
        values.put("expired_leases",jdbc.queryForObject("""
                SELECT count(*) FROM jobs j JOIN job_attempts a ON a.id=j.active_attempt_id
                WHERE j.status='RUNNING' AND a.lease_until<=clock_timestamp()
                """,Map.of(),Double.class));
        return new Snapshot(Map.copyOf(values),Map.copyOf(timings));
    }
}
