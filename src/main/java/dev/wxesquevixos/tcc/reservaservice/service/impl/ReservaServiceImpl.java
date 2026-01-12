package dev.wxesquevixos.tcc.reservaservice.service.impl;

import dev.wxesquevixos.tcc.reservaservice.domain.ReservaEntity;
import dev.wxesquevixos.tcc.reservaservice.domain.ReservaStatus;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaAereaSolicitarRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaCreateRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaUpdateRequest;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteSnapshot;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteValidadoData;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ReservaCriadaData;
import dev.wxesquevixos.tcc.reservaservice.kafka.producer.ClienteValidationProducer;
import dev.wxesquevixos.tcc.reservaservice.kafka.producer.ReservaProducer;
import dev.wxesquevixos.tcc.reservaservice.mapper.ReservaMapper;
import dev.wxesquevixos.tcc.reservaservice.repository.ReservaRepository;
import dev.wxesquevixos.tcc.reservaservice.service.ReservaService;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
public class ReservaServiceImpl implements ReservaService {

    private final ReservaRepository repository;
    private final WebClient webClient;
    private final ReservaProducer producer;
    private final ClienteValidationProducer clienteValidationProducer;

    public ReservaServiceImpl(
            ReservaRepository repository,
            WebClient webClient,
            ReservaProducer producer,
            ClienteValidationProducer clienteValidationProducer
    ) {
        this.repository = repository;
        this.webClient = webClient;
        this.producer = producer;
        this.clienteValidationProducer = clienteValidationProducer;
    }

    @Override
    public Mono<ReservaEntity> create(ReservaCreateRequest req) {
        var entity = ReservaMapper.toEntity(req);

        var now = OffsetDateTime.now();
        var moeda = (entity.moeda() == null || entity.moeda().isBlank()) ? "BRL" : entity.moeda();
        var status = (entity.status() == null) ? ReservaStatus.PENDING : entity.status();

        // ✅ Agora sem destinatario e com snapshot (pode ser null no CRUD)
        var toSave = new ReservaEntity(
                null,
                entity.clienteId(),
                entity.vooId(),
                status,
                entity.valorTotal(),
                moeda,
                entity.metodo(),
                entity.motivoCancelamento(),
                entity.correlationId(),
                now,
                now,
                entity.clientePaymentToken(),
                entity.clienteEmail(),
                entity.clienteNome()
        );

        return validateClienteExternally(toSave.clienteId())
                .then(repository.existsByCorrelationId(toSave.correlationId()))
                .flatMap(exists -> exists
                        ? repository.findByCorrelationId(toSave.correlationId()) // idempotência
                        : repository.save(toSave)
                );
    }

    @Override
    public Mono<ReservaEntity> findById(Long id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Reserva não encontrada")));
    }

    @Override
    public Mono<ReservaEntity> findByCorrelationId(UUID correlationId) {
        return repository.findByCorrelationId(correlationId)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Reserva não encontrada para correlationId")));
    }

    @Override
    public Flux<ReservaEntity> findAll() {
        return repository.findAll();
    }

    @Override
    public Mono<ReservaEntity> update(Long id, ReservaUpdateRequest req) {
        return findById(id)
                .flatMap(existing -> {

                    var updated = new ReservaEntity(
                            existing.id(),
                            req.clienteId() != null ? req.clienteId() : existing.clienteId(),
                            existing.vooId(), // não atualiza aqui
                            req.status() != null ? req.status() : existing.status(),
                            req.valorTotal() != null ? req.valorTotal() : existing.valorTotal(),
                            (req.moeda() != null && !req.moeda().isBlank()) ? req.moeda() : existing.moeda(),
                            existing.metodo(), // não atualiza aqui
                            req.motivoCancelamento() != null ? req.motivoCancelamento() : existing.motivoCancelamento(),
                            existing.correlationId(),
                            existing.criadoEm(),
                            OffsetDateTime.now(),

                            // snapshot fica como está (CRUD não “reinventa” snapshot)
                            existing.clientePaymentToken(),
                            existing.clienteEmail(),
                            existing.clienteNome()
                    );

                    return repository.save(updated);
                });
    }

    @Override
    public Mono<Void> delete(Long id) {
        return findById(id).then(repository.deleteById(id));
    }

    /**
     * Mantido como estava (hoje retorna empty).
     * Se quiser, pode remover WebClient depois.
     */
    private Mono<Void> validateClienteExternally(Long clienteId) {
        return Mono.empty();
    }

    /**
     * Fluxo SAGA:
     * 1) cria reserva PENDING_VALIDATION
     * 2) publica VALIDAR_CLIENTE
     * 3) publica RESERVA_PENDENTE_VALIDACAO (para notificação)
     *
     * Obs: aqui ainda podemos usar req.destinatario() apenas para a notificação
     * “em validação”, mas NÃO persistimos esse campo na reserva.
     */
    public Mono<ReservaEntity> solicitarCompraAerea(ReservaAereaSolicitarRequest req) {

        var correlationId = req.correlationId() != null ? req.correlationId() : UUID.randomUUID();
        var moeda = (req.moeda() == null || req.moeda().isBlank()) ? "BRL" : req.moeda();
        var now = OffsetDateTime.now();

        var toSave = new ReservaEntity(
                null,
                req.clienteId(),
                req.vooId(),
                ReservaStatus.PENDING_VALIDATION,
                req.valor(),
                moeda,
                req.metodo(),
                null, // motivoCancelamento
                correlationId,
                now,
                now,

                // snapshot ainda não existe aqui
                null,
                null,
                null
        );

        return repository.existsByCorrelationId(correlationId)
                .flatMap(exists -> exists
                        ? repository.findByCorrelationId(correlationId) // idempotência
                        : repository.save(toSave)
                )
                .doOnNext(saved -> {
                    // 1) validação do cliente
                    clienteValidationProducer.publicarValidarCliente(saved.id(), saved.clienteId(), correlationId);

                    // 2) notificação "em validação" (usa destinatario do request)
//                    producer.publicarReservaPendenteValidacao(
//                            saved.id(),
//                            correlationId,
//                            saved.clienteEmail()
//                    );
                });
    }

    @Override
    public Mono<Void> onClienteValidado(EventEnvelope env, ClienteValidadoData data) {

        final UUID correlationId = env.correlationId();
        final ClienteSnapshot snapshot = data.snapshot(); // pode vir null

        return repository.findById(data.reservaId())
                .switchIfEmpty(Mono.error(new IllegalStateException("Reserva não encontrada: " + data.reservaId())))
                .flatMap(existing -> {

                    // snapshot “normalizado” para persistir (sem NPE)
                    final String paymentToken = snapshot != null ? snapshot.paymentToken() : null;
                    final String email = snapshot != null ? snapshot.email() : null;
                    final String nome = snapshot != null ? snapshot.nome() : null;

                    if (!data.valido()) {

                        var cancelled = new ReservaEntity(
                                existing.id(),
                                existing.clienteId(),
                                existing.vooId(),
                                ReservaStatus.CANCELLED,
                                existing.valorTotal(),
                                existing.moeda(),
                                existing.metodo(),
                                data.motivo() != null ? data.motivo() : "CLIENTE_INVALIDO",
                                existing.correlationId(),
                                existing.criadoEm(),
                                OffsetDateTime.now(),
                                paymentToken,
                                email,
                                nome
                        );

                        return repository.save(cancelled)
                                .doOnSuccess(saved ->
                                        producer.publicarReservaCancelada(
                                                saved.id(),
                                                correlationId,
                                                saved.motivoCancelamento(),
                                                snapshot
                                        )
                                )
                                .then();
                    }

                    // ✅ cliente válido -> reserva pronta para seguir (PENDING) e snapshot persistido
                    var pending = new ReservaEntity(
                            existing.id(),
                            existing.clienteId(),
                            existing.vooId(),
                            ReservaStatus.PENDING,
                            existing.valorTotal(),
                            existing.moeda(),
                            existing.metodo(),
                            null,
                            existing.correlationId(),
                            existing.criadoEm(),
                            OffsetDateTime.now(),
                            paymentToken,
                            email,
                            nome
                    );

                    return repository.save(pending)
                            .doOnSuccess(saved -> producer.publicarReservaCriada(
                                    new ReservaCriadaData(
                                            saved.id(),
                                            saved.vooId(),
                                            saved.valorTotal(),
                                            saved.moeda(),
                                            saved.metodo(),
                                            snapshot,
                                            correlationId
                                    ),
                                    correlationId
                            ))
                            .then();
                });
    }
}