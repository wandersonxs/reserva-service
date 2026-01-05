package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String type,
        UUID correlationId,
        OffsetDateTime occurredAt,
        String source,
        Map<String, Object> data
) {}