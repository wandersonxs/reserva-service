package dev.wxesquevixos.tcc.reservaservice.kafka.producer;

import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteValidationEventTypes;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@Component
public class ClienteValidationProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;
    private final String source;

    public ClienteValidationProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.cliente-validation}") String topic,
            @Value("${app.kafka.source}") String source
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.source = source;
    }

    public void publicarValidarCliente(Long reservaId, Long clienteId, UUID correlationId) {
        var env = new EventEnvelope(
                UUID.randomUUID(),
                ClienteValidationEventTypes.VALIDAR_CLIENTE,
                correlationId,
                OffsetDateTime.now(),
                source,
                Map.of(
                        "reservaId", reservaId,
                        "clienteId", clienteId,
                        "correlationId", correlationId
                )
        );

        // chave = correlationId (você já usa assim). Alternativa: reservaId
        kafkaTemplate.send(topic, correlationId.toString(), env);
    }
}