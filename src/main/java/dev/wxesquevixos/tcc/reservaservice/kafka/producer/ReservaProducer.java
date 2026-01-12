package dev.wxesquevixos.tcc.reservaservice.kafka.producer;

import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteSnapshot;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ReservaCriadaData;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ReservaEventTypes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class ReservaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String reservaEventsTopic;
    private final String source;

    public ReservaProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.reserva-events}") String reservaEventsTopic,
            @Value("${app.kafka.source}") String source
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.reservaEventsTopic = reservaEventsTopic;
        this.source = source;
    }

    public void publicarReservaPendenteValidacao(Long reservaId, UUID correlationId, String destinatario) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("reservaId", reservaId);
        payload.put("destinatario", destinatario);
        payload.put("correlationId", correlationId);

        var env = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.PENDING_VALIDATION,
                correlationId,
                OffsetDateTime.now(),
                source,
                payload
        );

        kafkaTemplate.send(reservaEventsTopic, correlationId.toString(), env);
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

        var env = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.RESERVA_CRIADA,
                correlationId,
                OffsetDateTime.now(),
                source,
                payload
        );

        kafkaTemplate.send(reservaEventsTopic, correlationId.toString(), env);
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

        var env = new EventEnvelope(
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

        kafkaTemplate.send(reservaEventsTopic, correlationId.toString(), env);
    }

    public void publicarReservaCancelada(Long reservaId, UUID correlationId, String motivo, ClienteSnapshot snapshot) {

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

        var env = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.RESERVA_CANCELADA,
                correlationId,
                OffsetDateTime.now(),
                source,
                payload
        );

        kafkaTemplate.send(reservaEventsTopic, correlationId.toString(), env);
    }
}