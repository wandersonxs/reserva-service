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

        Observation obs = null;

        try {
            final EventEnvelope env = objectMapper.convertValue(record.value(), EventEnvelope.class);
            final UUID correlationId = env.correlationId(); // seu saga.id

            if (correlationId != null) {
                MDC.put("correlationId", correlationId.toString());
            }

            // ✅ extrai traceparent/baggage dos headers do Kafka
            Context extractedParent = PROPAGATOR.extract(Context.current(), record.headers(), KAFKA_HEADERS_GETTER);

            // ✅ cria o span/observation como filho do contexto extraído
            try (Scope scope = extractedParent.makeCurrent()) {

                obs = Observation.start("saga.step.pagamento->reserva", observationRegistry)
                        .lowCardinalityKeyValue("service", "reserva-service")
                        .lowCardinalityKeyValue("saga", "reserva")
                        .lowCardinalityKeyValue("step", "pagamento")
                        .lowCardinalityKeyValue("saga.id", correlationId != null ? correlationId.toString() : "null")
                        .lowCardinalityKeyValue("event.type", String.valueOf(env.type()))
                        .lowCardinalityKeyValue("topic", record.topic())
                        .lowCardinalityKeyValue("messaging.system", "kafka")
                        .lowCardinalityKeyValue("messaging.operation", "process");

                final Observation finalObs = obs;

                Mono<Void> pipeline = switch (env.type()) {

                    case "PAGAMENTO_AUTORIZADO", "PAGAMENTO_CAPTURADO" ->
                            repository.findByCorrelationId(correlationId)
                                    .switchIfEmpty(Mono.error(new IllegalStateException(
                                            "Reserva não encontrada para correlationId=" + correlationId)))
                                    .flatMap(reserva -> {

                                        final ReservaEntity updated = new ReservaEntity(
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

                                        final ClienteSnapshot snapshot = new ClienteSnapshot(
                                                updated.clientePaymentToken(),
                                                updated.clienteEmail(),
                                                updated.clienteNome()
                                        );

                                        return repository.save(updated)
                                                .doOnNext(saved ->
                                                        producer.publicarReservaConfirmada(
                                                                saved.id(),
                                                                saved.vooId(),
                                                                saved.valorTotal(),
                                                                saved.moeda(),
                                                                saved.metodo(),
                                                                saved.correlationId(),
                                                                snapshot
                                                        )
                                                )
                                                .then();
                                    });

                    case "PAGAMENTO_RECUSADO", "PAGAMENTO_FALHOU" ->
                            repository.findByCorrelationId(correlationId)
                                    .switchIfEmpty(Mono.error(new IllegalStateException(
                                            "Reserva não encontrada para correlationId=" + correlationId)))
                                    .flatMap(reserva -> {

                                        final String motivo = extractMotivo(env);

                                        final ReservaEntity updated = new ReservaEntity(
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

                                        final ClienteSnapshot snapshot = new ClienteSnapshot(
                                                updated.clientePaymentToken(),
                                                updated.clienteEmail(),
                                                updated.clienteNome()
                                        );

                                        return repository.save(updated)
                                                .doOnNext(saved ->
                                                        producer.publicarReservaCancelada(
                                                                saved.id(),
                                                                saved.correlationId(),
                                                                motivo,
                                                                snapshot
                                                        )
                                                )
                                                .then();
                                    });

                    default -> Mono.empty();
                };

                pipeline
                        .doOnSuccess(v -> ack.acknowledge())
                        .doOnError(ex -> {
                            if (finalObs != null) finalObs.error(ex);

                            log.error("Erro processando pagamento.events. topic={} partition={} offset={} key={} correlationId={} msg={}",
                                    record.topic(), record.partition(), record.offset(), record.key(),
                                    correlationId, ex.getMessage(), ex);

                            ack.acknowledge(); // TCC: descarta
                        })
                        .doFinally(sig -> {
                            try {
                                if (finalObs != null) finalObs.stop();
                            } finally {
                                MDC.remove("correlationId");
                            }
                        })
                        .subscribe();
            }

        } catch (Exception ex) {

            // ✅ mesmo se der erro de parse, ainda dá pra tentar extrair o contexto do header
            Context extractedParent = PROPAGATOR.extract(Context.current(), record.headers(), KAFKA_HEADERS_GETTER);

            try (Scope scope = extractedParent.makeCurrent()) {
                Observation invalidObs = Observation.start("kafka.message.invalid", observationRegistry)
                        .lowCardinalityKeyValue("service", "reserva-service")
                        .lowCardinalityKeyValue("topic", record.topic())
                        .lowCardinalityKeyValue("messaging.system", "kafka")
                        .lowCardinalityKeyValue("messaging.operation", "process");

                invalidObs.error(ex);

                log.warn("Mensagem inválida em pagamento.events, descartando. topic={} partition={} offset={} key={} msg={}",
                        record.topic(), record.partition(), record.offset(), record.key(),
                        ex.getMessage(), ex);

                ack.acknowledge();
                invalidObs.stop();
            } finally {
                MDC.remove("correlationId");
            }
        }
    }

    private static String extractMotivo(EventEnvelope env) {
        try {
            if (env.data() instanceof Map<?, ?> map) {
                Object v = map.get("motivo");
                return v != null ? v.toString() : "Pagamento recusado/falhou";
            }
        } catch (Exception ignored) {}
        return "Pagamento recusado/falhou";
    }
}