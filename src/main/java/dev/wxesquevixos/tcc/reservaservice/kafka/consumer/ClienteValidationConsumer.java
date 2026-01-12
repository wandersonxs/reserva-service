package dev.wxesquevixos.tcc.reservaservice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteValidadoData;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteValidationEventTypes;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import dev.wxesquevixos.tcc.reservaservice.service.ReservaService;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class ClienteValidationConsumer {

    private static final Logger log = LoggerFactory.getLogger(ClienteValidationConsumer.class);

    private static final TextMapPropagator PROPAGATOR =
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator();

    private static final TextMapGetter<Headers> KAFKA_HEADERS_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers headers) {
            if (headers == null) return List.of();
            List<String> keys = new ArrayList<>();
            for (Header h : headers) keys.add(h.key());
            return keys;
        }

        @Override
        public String get(Headers headers, String key) {
            if (headers == null || key == null) return null;
            Header h = headers.lastHeader(key);
            if (h == null || h.value() == null) return null;
            return new String(h.value(), StandardCharsets.UTF_8);
        }
    };

    private final ObjectMapper objectMapper;
    private final ReservaService reservaService;
    private final ObservationRegistry observationRegistry;

    public ClienteValidationConsumer(
            ObjectMapper objectMapper,
            ReservaService reservaService,
            ObservationRegistry observationRegistry
    ) {
        this.objectMapper = objectMapper;
        this.reservaService = reservaService;
        this.observationRegistry = observationRegistry;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.cliente-validation}",
            groupId = "${app.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {

        Observation obs = null;

        try {
            final EventEnvelope env = objectMapper.convertValue(record.value(), EventEnvelope.class);
            final UUID correlationId = env.correlationId();

            if (correlationId != null) {
                MDC.put("correlationId", correlationId.toString());
            }

            if (!ClienteValidationEventTypes.CLIENTE_VALIDADO.equals(env.type())) {
                log.info("Ignorando evento em cliente.validation: type={} correlationId={}", env.type(), correlationId);
                ack.acknowledge();
                return;
            }

            final ClienteValidadoData data = objectMapper.convertValue(env.data(), ClienteValidadoData.class);

            // ✅ Extrai o contexto do trace dos headers do Kafka (traceparent/baggage)
            Context extractedParent = PROPAGATOR.extract(Context.current(), record.headers(), KAFKA_HEADERS_GETTER);

            // ✅ Tudo que iniciar aqui dentro herda o traceId do producer
            try (Scope otelScope = extractedParent.makeCurrent()) {

                obs = Observation.start("saga.step.cliente-validation", observationRegistry)
                        .lowCardinalityKeyValue("saga.id", correlationId != null ? correlationId.toString() : "null")
                        .lowCardinalityKeyValue("event.type", String.valueOf(env.type()))
                        .lowCardinalityKeyValue("topic", record.topic())
                        .lowCardinalityKeyValue("consumer", "cliente.validation")
                        .lowCardinalityKeyValue("messaging.system", "kafka")
                        .lowCardinalityKeyValue("messaging.operation", "process");

                final Observation finalObs = obs;

                Mono<Void> pipeline = reservaService.onClienteValidado(env, data);

                pipeline
                        .doOnSuccess(v -> ack.acknowledge())
                        .doOnError(ex -> {
                            if (finalObs != null) finalObs.error(ex);

                            log.error("Erro processando CLIENTE_VALIDADO. topic={} partition={} offset={} key={} correlationId={} msg={}",
                                    record.topic(), record.partition(), record.offset(), record.key(),
                                    correlationId, ex.getMessage(), ex);

                            ack.acknowledge(); // TCC: descarta
                        })
                        .doFinally(signal -> {
                            try {
                                if (finalObs != null) finalObs.stop();
                            } finally {
                                MDC.remove("correlationId");
                            }
                        })
                        .subscribe();
            }

        } catch (Exception ex) {
            if (obs != null) obs.error(ex);

            log.error("Erro no consumer cliente.validation (parse/process). topic={} partition={} offset={} key={} msg={}",
                    record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage(), ex);

            ack.acknowledge();

            if (obs != null) obs.stop();
            MDC.remove("correlationId");
        }
    }
}