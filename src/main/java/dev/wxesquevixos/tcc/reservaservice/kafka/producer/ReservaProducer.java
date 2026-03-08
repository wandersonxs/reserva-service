package dev.wxesquevixos.tcc.reservaservice.kafka.producer;

import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteSnapshot;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ReservaCriadaData;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ReservaEventTypes;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class ReservaProducer {

    private static final TextMapPropagator PROPAGATOR =
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator();

    private static final TextMapSetter<Headers> KAFKA_HEADERS_SETTER = (headers, key, value) -> {
        if (headers == null || key == null || value == null) {
            return;
        }
        headers.remove(key);
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    };

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObservationRegistry observationRegistry;
    private final MeterRegistry meterRegistry;
    private final String reservaEventsTopic;
    private final String source;

    private final Counter sagaEndConfirmed;
    private final Counter sagaEndCancelled;

    public ReservaProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            ObservationRegistry observationRegistry,
            MeterRegistry meterRegistry,
            @Value("${app.kafka.topics.reserva-events}") String reservaEventsTopic,
            @Value("${app.kafka.source}") String source
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.observationRegistry = observationRegistry;
        this.meterRegistry = meterRegistry;
        this.reservaEventsTopic = reservaEventsTopic;
        this.source = source;

        this.sagaEndConfirmed = Counter.builder("saga_end_total")
                .description("Total de fins de saga publicados pelo reserva-service")
                .tag("service", "reserva-service")
                .tag("event_type", ReservaEventTypes.RESERVA_CONFIRMADA)
                .register(meterRegistry);

        this.sagaEndCancelled = Counter.builder("saga_end_total")
                .description("Total de fins de saga publicados pelo reserva-service")
                .tag("service", "reserva-service")
                .tag("event_type", ReservaEventTypes.RESERVA_CANCELADA)
                .register(meterRegistry);
    }

    public void publicarReservaPendenteValidacao(Long reservaId, UUID correlationId, String destinatario) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("reservaId", reservaId);
        payload.put("destinatario", destinatario);
        payload.put("correlationId", correlationId);

        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.PENDING_VALIDATION,
                correlationId,
                OffsetDateTime.now(),
                source,
                payload
        );

        sendObserved(
                reservaEventsTopic,
                ReservaEventTypes.PENDING_VALIDATION,
                correlationId,
                envelope
        );
    }

    public void publicarReservaCriada(ReservaCriadaData data, UUID correlationId) {
        Map<String, Object> snapshotMap = null;
        if (data.snapshot() != null) {
            snapshotMap = new HashMap<>();
            snapshotMap.put("paymentToken", data.snapshot().paymentToken());
            snapshotMap.put("email", data.snapshot().email());
            snapshotMap.put("nome", data.snapshot().nome());
        }

        Map<String, Object> payload = new HashMap<>();
        payload.put("reservaId", data.reservaId());
        payload.put("vooId", data.vooId());
        payload.put("valor", data.valor());
        payload.put("moeda", data.moeda());
        payload.put("metodo", data.metodo());
        payload.put("snapshot", snapshotMap);
        payload.put("correlationId", data.correlationId());

        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.RESERVA_CRIADA,
                correlationId,
                OffsetDateTime.now(),
                source,
                payload
        );

        sendObserved(
                reservaEventsTopic,
                ReservaEventTypes.RESERVA_CRIADA,
                correlationId,
                envelope
        );
    }

    public void publicarReservaConfirmada(
            Long reservaId,
            Long vooId,
            java.math.BigDecimal valor,
            String moeda,
            String metodo,
            UUID correlationId,
            ClienteSnapshot snapshot
    ) {
        if (snapshot == null || snapshot.email() == null || snapshot.email().isBlank()) {
            throw new IllegalArgumentException("snapshot.email é obrigatório para RESERVA_CONFIRMADA");
        }

        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.RESERVA_CONFIRMADA,
                correlationId,
                OffsetDateTime.now(),
                source,
                Map.of(
                        "reservaId", reservaId,
                        "vooId", vooId,
                        "valor", valor,
                        "moeda", moeda,
                        "metodo", metodo,
                        "snapshot", Map.of(
                                "paymentToken", snapshot.paymentToken(),
                                "email", snapshot.email(),
                                "nome", snapshot.nome()
                        )
                )
        );

        sendObserved(
                reservaEventsTopic,
                ReservaEventTypes.RESERVA_CONFIRMADA,
                correlationId,
                envelope
        );
    }

    public void publicarReservaCancelada(
            Long reservaId,
            UUID correlationId,
            String motivo,
            ClienteSnapshot snapshot
    ) {
        Map<String, Object> snapshotMap = null;
        if (snapshot != null) {
            snapshotMap = new HashMap<>();
            snapshotMap.put("paymentToken", snapshot.paymentToken());
            snapshotMap.put("email", snapshot.email());
            snapshotMap.put("nome", snapshot.nome());
        }

        Map<String, Object> payload = new HashMap<>();
        payload.put("reservaId", reservaId);
        payload.put("motivo", motivo);
        payload.put("snapshot", snapshotMap);
        payload.put("correlationId", correlationId);

        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.RESERVA_CANCELADA,
                correlationId,
                OffsetDateTime.now(),
                source,
                payload
        );

        sendObserved(
                reservaEventsTopic,
                ReservaEventTypes.RESERVA_CANCELADA,
                correlationId,
                envelope
        );
    }

    private void sendObserved(String topic, String eventType, UUID correlationId, Object payload) {
        if (correlationId != null) {
            MDC.put("correlationId", correlationId.toString());
        }

        Observation observation = Observation.createNotStarted(
                        "saga.publish." + eventType.toLowerCase().replace('_', '-'),
                        observationRegistry
                )
                .lowCardinalityKeyValue("messaging.system", "kafka")
                .lowCardinalityKeyValue("messaging.operation", "send")
                .lowCardinalityKeyValue("topic", topic)
                .lowCardinalityKeyValue("event.type", eventType)
                .lowCardinalityKeyValue("producer", "reserva-service")
                .lowCardinalityKeyValue(
                        "correlationId",
                        correlationId != null ? correlationId.toString() : "null"
                )
                .highCardinalityKeyValue(
                        "correlationId",
                        correlationId != null ? correlationId.toString() : "null"
                );

        try {
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(topic, correlationId != null ? correlationId.toString() : null, payload);

            observation.start();
            try (Observation.Scope scope = observation.openScope()) {
                PROPAGATOR.inject(Context.current(), record.headers(), KAFKA_HEADERS_SETTER);

                kafkaTemplate.send(record)
                        .whenComplete((result, ex) -> {
                            try {
                                if (ex != null) {
                                    observation.error(ex);
                                } else {
                                    if (ReservaEventTypes.RESERVA_CONFIRMADA.equals(eventType)) {
                                        sagaEndConfirmed.increment();
                                    } else if (ReservaEventTypes.RESERVA_CANCELADA.equals(eventType)) {
                                        sagaEndCancelled.increment();
                                    }
                                }
                            } finally {
                                observation.stop();
                            }
                        });
            }

        } catch (Exception ex) {
            observation.error(ex);
            observation.stop();
            throw ex;
        } finally {
            MDC.remove("correlationId");
        }
    }
}