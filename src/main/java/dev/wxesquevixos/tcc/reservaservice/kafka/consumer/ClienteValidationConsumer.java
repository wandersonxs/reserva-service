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
            if (headers == null) {
                return List.of();
            }

            List<String> keys = new ArrayList<>();
            for (Header header : headers) {
                keys.add(header.key());
            }
            return keys;
        }

        @Override
        public String get(Headers headers, String key) {
            if (headers == null || key == null) {
                return null;
            }

            Header header = headers.lastHeader(key);
            if (header == null || header.value() == null) {
                return null;
            }

            return new String(header.value(), StandardCharsets.UTF_8);
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
        Observation observation = null;

        try {
            EventEnvelope envelope = objectMapper.convertValue(record.value(), EventEnvelope.class);
            UUID correlationId = envelope.correlationId();

            if (correlationId != null) {
                MDC.put("correlationId", correlationId.toString());
            }

            if (!envelope.type().equalsIgnoreCase(ClienteValidationEventTypes.CLIENTE_VALIDADO.name())) {
                log.info("Ignorando evento em cliente.validation: type={} correlationId={}",
                        envelope.type(), correlationId);
                ack.acknowledge();
                return;
            }

            ClienteValidadoData data =
                    objectMapper.convertValue(envelope.data(), ClienteValidadoData.class);

            Context extractedParent =
                    PROPAGATOR.extract(Context.current(), record.headers(), KAFKA_HEADERS_GETTER);

            try (Scope ignored = extractedParent.makeCurrent()) {
                observation = Observation.createNotStarted(
                                "saga.consume.cliente-validado",
                                observationRegistry
                        )
                        .lowCardinalityKeyValue("messaging.system", "kafka")
                        .lowCardinalityKeyValue("messaging.operation", "process")
                        .lowCardinalityKeyValue("topic", record.topic())
                        .lowCardinalityKeyValue("event.type", ClienteValidationEventTypes.CLIENTE_VALIDADO.name())
                        .lowCardinalityKeyValue("consumer", "reserva-service")
                        .highCardinalityKeyValue(
                                "correlationId",
                                correlationId != null ? correlationId.toString() : "null"
                        );

                Observation finalObservation = observation;
                finalObservation.start();

                Mono<Void> pipeline = reservaService.onClienteValidado(envelope, data);

                pipeline
                        .doOnSuccess(ignoredSignal -> ack.acknowledge())
                        .doOnError(ex -> {
                            finalObservation.error(ex);

                            log.error(
                                    "Erro processando CLIENTE_VALIDADO. topic={} partition={} offset={} key={} correlationId={} msg={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    correlationId,
                                    ex.getMessage(),
                                    ex
                            );

                            ack.acknowledge(); // TCC: descarta
                        })
                        .doFinally(signalType -> {
                            try {
                                finalObservation.stop();
                            } finally {
                                MDC.remove("correlationId");
                            }
                        })
                        .subscribe();
            }

        } catch (Exception ex) {
            if (observation != null) {
                observation.error(ex);
                observation.stop();
            }

            log.error(
                    "Erro no consumer cliente.validation (parse/process). topic={} partition={} offset={} key={} msg={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    ex.getMessage(),
                    ex
            );

            ack.acknowledge();
            MDC.remove("correlationId");
        }
    }
}