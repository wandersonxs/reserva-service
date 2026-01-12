package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String type,
        UUID correlationId,
        OffsetDateTime occurredAt,
        String source,
        @JsonProperty("data") Map<String, Object> data
) {}