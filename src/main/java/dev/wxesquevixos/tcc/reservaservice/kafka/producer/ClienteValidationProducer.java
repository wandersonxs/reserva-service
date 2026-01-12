package dev.wxesquevixos.tcc.reservaservice.kafka.producer;

import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteValidationEventTypes;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
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
import java.util.Map;
import java.util.UUID;

@Component
public class ClienteValidationProducer {

    private static final TextMapPropagator PROPAGATOR =
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator();

    private static final TextMapSetter<Headers> KAFKA_HEADERS_SETTER = (headers, key, value) -> {
        if (headers == null || key == null || value == null) return;
        headers.remove(key);
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    };

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObservationRegistry observationRegistry;
    private final String topic;
    private final String source;

    public ClienteValidationProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            ObservationRegistry observationRegistry,
            @Value("${app.kafka.topics.cliente-validation}") String topic,
            @Value("${app.kafka.source}") String source
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.observationRegistry = observationRegistry;
        this.topic = topic;
        this.source = source;
    }

    public void publicarValidarCliente(Long reservaId, Long clienteId, UUID correlationId) {

        MDC.put("correlationId", correlationId.toString());

        Observation obs = Observation.start("saga.publish.cliente-validation", observationRegistry)
                .lowCardinalityKeyValue("messaging.system", "kafka")
                .lowCardinalityKeyValue("messaging.operation", "send")
                .lowCardinalityKeyValue("topic", topic)
                .lowCardinalityKeyValue("event.type", ClienteValidationEventTypes.VALIDAR_CLIENTE)
                .lowCardinalityKeyValue("producer", "cliente.validation")
                .lowCardinalityKeyValue("saga.id", correlationId.toString());

        try (Observation.Scope scope = obs.openScope()) {

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

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(topic, correlationId.toString(), env);

            // ✅ injeta trace context nos headers Kafka
            PROPAGATOR.inject(Context.current(), record.headers(), KAFKA_HEADERS_SETTER);

            kafkaTemplate.send(record).whenComplete((result, ex) -> {
                try {
                    if (ex != null) obs.error(ex);
                } finally {
                    obs.stop();
                    MDC.remove("correlationId");
                }
            });

        } catch (Exception ex) {
            obs.error(ex);
            obs.stop();
            MDC.remove("correlationId");
            throw ex;
        }
    }
}