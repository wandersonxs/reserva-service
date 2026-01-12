package dev.wxesquevixos.tcc.reservaservice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.wxesquevixos.tcc.reservaservice.domain.ReservaEntity;
import dev.wxesquevixos.tcc.reservaservice.domain.ReservaStatus;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteSnapshot;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import dev.wxesquevixos.tcc.reservaservice.kafka.producer.ReservaProducer;
import dev.wxesquevixos.tcc.reservaservice.repository.ReservaRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@Component
public class PagamentoReservaConsumer {

    private static final Logger log = LoggerFactory.getLogger(PagamentoReservaConsumer.class);

    private final ReservaRepository repository;
    private final ObjectMapper objectMapper;
    private final ReservaProducer producer;

    public PagamentoReservaConsumer(
            ReservaRepository repository,
            ObjectMapper objectMapper,
            ReservaProducer producer
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.producer = producer;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.pagamento-events}",
            groupId = "${app.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {

        try {
            final EventEnvelope env = objectMapper.convertValue(record.value(), EventEnvelope.class);
            final UUID correlationId = env.correlationId();

            Mono<Void> pipeline = switch (env.type()) {

                // ✅ Pagamento aprovado -> confirma reserva + publica RESERVA_CONFIRMADA (com snapshot)
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
                                            reserva.motivoCancelamento(), // continua null normalmente
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

                // ❌ Pagamento recusado/falhou -> cancela reserva + publica RESERVA_CANCELADA (com snapshot)
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
                        log.error("Erro processando pagamento.events. topic={} partition={} offset={} key={} correlationId={} msg={}",
                                record.topic(), record.partition(), record.offset(), record.key(),
                                correlationId, ex.getMessage(), ex);
                        ack.acknowledge(); // TCC: descarta
                    })
                    .subscribe();

        } catch (Exception ex) {
            log.warn("Mensagem inválida em pagamento.events, descartando. topic={} partition={} offset={} key={} msg={}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    ex.getMessage(), ex);
            ack.acknowledge();
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