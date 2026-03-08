package dev.wxesquevixos.tcc.reservaservice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.wxesquevixos.tcc.reservaservice.domain.ReservaEntity;
import dev.wxesquevixos.tcc.reservaservice.domain.ReservaStatus;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteSnapshot;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import dev.wxesquevixos.tcc.reservaservice.kafka.producer.ReservaProducer;
import dev.wxesquevixos.tcc.reservaservice.repository.ReservaRepository;
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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class PagamentoReservaConsumer {

    private static final Logger log = LoggerFactory.getLogger(PagamentoReservaConsumer.class);

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

    private final ReservaRepository repository;
    private final ObjectMapper objectMapper;
    private final ReservaProducer producer;
    private final ObservationRegistry observationRegistry;

    public PagamentoReservaConsumer(
            ReservaRepository repository,
            ObjectMapper objectMapper,
            ReservaProducer producer,
            ObservationRegistry observationRegistry
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.producer = producer;
        this.observationRegistry = observationRegistry;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.pagamento-events}",
            groupId = "${app.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {

        Observation invalidObservation = null;

        try {
            EventEnvelope envelope = objectMapper.convertValue(record.value(), EventEnvelope.class);
            UUID correlationId = envelope.correlationId();

            if (correlationId != null) {
                MDC.put("correlationId", correlationId.toString());
            }

            Context extractedParent =
                    PROPAGATOR.extract(Context.current(), record.headers(), KAFKA_HEADERS_GETTER);

            try (Scope ignored = extractedParent.makeCurrent()) {

                if ("PAGAMENTO_AUTORIZADO".equals(envelope.type())
                        || "PAGAMENTO_CAPTURADO".equals(envelope.type())) {
                    processarPagamentoConfirmado(record, ack, envelope, correlationId, extractedParent);
                    return;
                }

                if ("PAGAMENTO_RECUSADO".equals(envelope.type())
                        || "PAGAMENTO_FALHOU".equals(envelope.type())) {
                    processarPagamentoFalhouOuRecusado(record, ack, envelope, correlationId, extractedParent);
                    return;
                }

                Observation ignoredObservation = Observation.createNotStarted(
                                "saga.consume.evento-ignorado",
                                observationRegistry
                        )
                        .lowCardinalityKeyValue("messaging.system", "kafka")
                        .lowCardinalityKeyValue("messaging.operation", "process")
                        .lowCardinalityKeyValue("topic", record.topic())
                        .lowCardinalityKeyValue("event.type", String.valueOf(envelope.type()))
                        .lowCardinalityKeyValue("consumer", "reserva-service")
                        .lowCardinalityKeyValue(
                                "correlationId",
                                correlationId != null ? correlationId.toString() : "null"
                        )
                        .highCardinalityKeyValue(
                                "correlationId",
                                correlationId != null ? correlationId.toString() : "null"
                        );

                ignoredObservation.start();
                try {
                    log.info(
                            "Ignorando evento em pagamento.events: type={} correlationId={}",
                            envelope.type(),
                            correlationId
                    );
                    ack.acknowledge();
                } finally {
                    ignoredObservation.stop();
                }
            }

        } catch (Exception ex) {
            Context extractedParent =
                    PROPAGATOR.extract(Context.current(), record.headers(), KAFKA_HEADERS_GETTER);

            try (Scope ignored = extractedParent.makeCurrent()) {
                invalidObservation = Observation.createNotStarted(
                                "kafka.message.invalid",
                                observationRegistry
                        )
                        .lowCardinalityKeyValue("service", "reserva-service")
                        .lowCardinalityKeyValue("topic", record.topic())
                        .lowCardinalityKeyValue("messaging.system", "kafka")
                        .lowCardinalityKeyValue("messaging.operation", "process");

                invalidObservation.start();
                invalidObservation.error(ex);

                log.warn(
                        "Mensagem inválida em pagamento.events, descartando. topic={} partition={} offset={} key={} msg={}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        ex.getMessage(),
                        ex
                );

                ack.acknowledge();

            } finally {
                if (invalidObservation != null) {
                    invalidObservation.stop();
                }
                MDC.remove("correlationId");
            }
        }
    }

    private void processarPagamentoConfirmado(
            ConsumerRecord<String, Object> record,
            Acknowledgment ack,
            EventEnvelope envelope,
            UUID correlationId,
            Context extractedParent
    ) {
        Observation observation = Observation.createNotStarted(
                        "saga.consume.pagamento-confirmado",
                        observationRegistry
                )
                .lowCardinalityKeyValue("messaging.system", "kafka")
                .lowCardinalityKeyValue("messaging.operation", "process")
                .lowCardinalityKeyValue("topic", record.topic())
                .lowCardinalityKeyValue("event.type", String.valueOf(envelope.type()))
                .lowCardinalityKeyValue("consumer", "reserva-service")
                .lowCardinalityKeyValue(
                        "correlationId",
                        correlationId != null ? correlationId.toString() : "null"
                )
                .highCardinalityKeyValue(
                        "correlationId",
                        correlationId != null ? correlationId.toString() : "null"
                );

        Mono<Void> pipeline = Mono.defer(() -> {
                    observation.start();

                    return repository.findByCorrelationId(correlationId)
                            .switchIfEmpty(Mono.error(new IllegalStateException(
                                    "Reserva não encontrada para correlationId=" + correlationId)))
                            .flatMap(reserva -> {
                                ReservaEntity updated = new ReservaEntity(
                                        reserva.id(),
                                        reserva.clienteId(),
                                        reserva.vooId(),
                                        ReservaStatus.CONFIRMED,
                                        reserva.valorTotal(),
                                        reserva.moeda(),
                                        reserva.metodo(),
                                        reserva.motivoCancelamento(),
                                        reserva.correlationId(),
                                        reserva.criadoEm(),
                                        OffsetDateTime.now(),
                                        reserva.clientePaymentToken(),
                                        reserva.clienteEmail(),
                                        reserva.clienteNome()
                                );

                                ClienteSnapshot snapshot = new ClienteSnapshot(
                                        updated.clientePaymentToken(),
                                        updated.clienteEmail(),
                                        updated.clienteNome()
                                );

                                return repository.save(updated)
                                        .doOnNext(saved -> {
                                            try (Scope producerScope = extractedParent.makeCurrent()) {
                                                producer.publicarReservaConfirmada(
                                                        saved.id(),
                                                        saved.vooId(),
                                                        saved.valorTotal(),
                                                        saved.moeda(),
                                                        saved.metodo(),
                                                        saved.correlationId(),
                                                        snapshot
                                                );
                                            }
                                        })
                                        .then();
                            });
                })
                .doOnSuccess(ignoredSignal -> ack.acknowledge())
                .doOnError(ex -> {
                    observation.error(ex);

                    log.error(
                            "Erro processando pagamento confirmado. topic={} partition={} offset={} key={} correlationId={} msg={}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            correlationId,
                            ex.getMessage(),
                            ex
                    );

                    ack.acknowledge();
                })
                .doFinally(signalType -> {
                    try {
                        observation.stop();
                    } finally {
                        MDC.remove("correlationId");
                    }
                });

        pipeline.subscribe();
    }

    private void processarPagamentoFalhouOuRecusado(
            ConsumerRecord<String, Object> record,
            Acknowledgment ack,
            EventEnvelope envelope,
            UUID correlationId,
            Context extractedParent
    ) {
        Observation observation = Observation.createNotStarted(
                        "saga.consume.pagamento-falhou-ou-recusado",
                        observationRegistry
                )
                .lowCardinalityKeyValue("messaging.system", "kafka")
                .lowCardinalityKeyValue("messaging.operation", "process")
                .lowCardinalityKeyValue("topic", record.topic())
                .lowCardinalityKeyValue("event.type", String.valueOf(envelope.type()))
                .lowCardinalityKeyValue("consumer", "reserva-service")
                .lowCardinalityKeyValue(
                        "correlationId",
                        correlationId != null ? correlationId.toString() : "null"
                )
                .highCardinalityKeyValue(
                        "correlationId",
                        correlationId != null ? correlationId.toString() : "null"
                );

        Mono<Void> pipeline = Mono.defer(() -> {
                    observation.start();

                    return repository.findByCorrelationId(correlationId)
                            .switchIfEmpty(Mono.error(new IllegalStateException(
                                    "Reserva não encontrada para correlationId=" + correlationId)))
                            .flatMap(reserva -> {
                                String motivo = extractMotivo(envelope);

                                ReservaEntity updated = new ReservaEntity(
                                        reserva.id(),
                                        reserva.clienteId(),
                                        reserva.vooId(),
                                        ReservaStatus.CANCELLED,
                                        reserva.valorTotal(),
                                        reserva.moeda(),
                                        reserva.metodo(),
                                        motivo,
                                        reserva.correlationId(),
                                        reserva.criadoEm(),
                                        OffsetDateTime.now(),
                                        reserva.clientePaymentToken(),
                                        reserva.clienteEmail(),
                                        reserva.clienteNome()
                                );

                                ClienteSnapshot snapshot = new ClienteSnapshot(
                                        updated.clientePaymentToken(),
                                        updated.clienteEmail(),
                                        updated.clienteNome()
                                );

                                return repository.save(updated)
                                        .doOnNext(saved -> {
                                            try (Scope producerScope = extractedParent.makeCurrent()) {
                                                producer.publicarReservaCancelada(
                                                        saved.id(),
                                                        saved.correlationId(),
                                                        motivo,
                                                        snapshot
                                                );
                                            }
                                        })
                                        .then();
                            });
                })
                .doOnSuccess(ignoredSignal -> ack.acknowledge())
                .doOnError(ex -> {
                    observation.error(ex);

                    log.error(
                            "Erro processando pagamento recusado/falhou. topic={} partition={} offset={} key={} correlationId={} msg={}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            correlationId,
                            ex.getMessage(),
                            ex
                    );

                    ack.acknowledge();
                })
                .doFinally(signalType -> {
                    try {
                        observation.stop();
                    } finally {
                        MDC.remove("correlationId");
                    }
                });

        pipeline.subscribe();
    }

    private static String extractMotivo(EventEnvelope envelope) {
        try {
            if (envelope.data() instanceof Map<?, ?> map) {
                Object motivo = map.get("motivo");
                return motivo != null ? motivo.toString() : "Pagamento recusado/falhou";
            }
        } catch (Exception ignored) {
        }
        return "Pagamento recusado/falhou";
    }
}