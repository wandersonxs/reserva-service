package dev.wxesquevixos.tcc.reservaservice.sagametrics;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class SagaEventLogger {

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;
    private final String serviceName;

    public SagaEventLogger(
            NamedParameterJdbcTemplate jdbc,
            @Value("${experiment.datasource.schema:observability}") String schema,
            @Value("${spring.application.name}") String serviceName
    ) {
        this.jdbc = jdbc;
        this.schema = schema;
        this.serviceName = serviceName;
    }

    public void log(
            String sagaId,
            String flow,
            String step,
            SagaEventType type,
            String result,
            String errorCode
    ) {
        String traceId = MDC.get("traceId");

        String sql = "insert into " + schema + ".saga_event_log " +
                "(saga_id, flow, service, step, event_type, result, error_code, trace_id, occurred_at) " +
                "values (:sagaId, :flow, :service, :step, :eventType, :result, :errorCode, :traceId, :occurredAt)";

        Map<String, Object> params = new HashMap<>();
        params.put("sagaId", sagaId);
        params.put("flow", flow);
        params.put("service", serviceName);
        params.put("step", step);
        params.put("eventType", type.name());
        params.put("result", result);
        params.put("errorCode", errorCode);
        params.put("traceId", traceId);
        params.put("occurredAt", OffsetDateTime.now());

        jdbc.update(sql, params);
    }
}