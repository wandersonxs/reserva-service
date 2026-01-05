package dev.wxesquevixos.tcc.reservaservice.kafka.producer;

import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ReservaCriadaData;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ReservaEventTypes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
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

    public void publicarReservaCriada(ReservaCriadaData data, UUID correlationId) {
        var env = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.RESERVA_CRIADA,
                correlationId,
                OffsetDateTime.now(),
                source,
                Map.of(
                        "reservaId", data.reservaId(),
                        "vooId", data.vooId(),
                        "valor", data.valor(),
                        "moeda", data.moeda(),
                        "metodo", data.metodo(),
                        "destinatario", data.destinatario(),
                        "correlationId", data.correlationId()
                )
        );

        kafkaTemplate.send(reservaEventsTopic, correlationId.toString(), env);
    }

    public void publicarReservaConfirmada(Long reservaId, UUID correlationId) {
        var env = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.RESERVA_CONFIRMADA,
                correlationId,
                OffsetDateTime.now(),
                source,
                Map.of("reservaId", reservaId, "correlationId", correlationId)
        );

        kafkaTemplate.send(reservaEventsTopic, correlationId.toString(), env);
    }

    public void publicarReservaCancelada(Long reservaId, UUID correlationId, String motivo) {
        var env = new EventEnvelope(
                UUID.randomUUID(),
                ReservaEventTypes.RESERVA_CANCELADA,
                correlationId,
                OffsetDateTime.now(),
                source,
                Map.of("reservaId", reservaId, "motivo", motivo, "correlationId", correlationId)
        );

        kafkaTemplate.send(reservaEventsTopic, correlationId.toString(), env);
    }
}