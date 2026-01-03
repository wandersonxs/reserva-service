package dev.wxesquevixos.tcc.reservaservice.service.impl;

import dev.wxesquevixos.tcc.reservaservice.domain.ReservaEntity;
import dev.wxesquevixos.tcc.reservaservice.domain.ReservaStatus;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaCreateRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaUpdateRequest;
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

    public ReservaServiceImpl(ReservaRepository repository, WebClient webClient) {
        this.repository = repository;
        this.webClient = webClient;
    }

    @Override
    public Mono<ReservaEntity> create(ReservaCreateRequest req) {
        var entity = ReservaMapper.toEntity(req);

        // defaults de aplicação (evita null e garante consistência)
        var now = OffsetDateTime.now();
        var moeda = entity.moeda() == null || entity.moeda().isBlank() ? "BRL" : entity.moeda();

        var toSave = new ReservaEntity(
                null,
                entity.clienteId(),
                entity.status() == null ? ReservaStatus.PENDING : entity.status(),
                entity.valorTotal(),
                "BRL",
                entity.correlationId(),
                now,
                now
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
                    // Regras mínimas: só atualiza campos permitidos e sempre atualiza "atualizadoEm"
                    var updated = new ReservaEntity(
                            existing.id(),
                            req.clienteId() != null ? req.clienteId() : existing.clienteId(),
                            req.status() != null ? req.status() : existing.status(),
                            req.valorTotal() != null ? req.valorTotal() : existing.valorTotal(),
                            (req.moeda() != null && !req.moeda().isBlank()) ? req.moeda() : existing.moeda(),
                            existing.correlationId(),
                            existing.criadoEm(),
                            OffsetDateTime.now()
                    );
                    return repository.save(updated);
                });
    }

    @Override
    public Mono<Void> delete(Long id) {
        return findById(id).then(repository.deleteById(id));
    }

    /**
     * Exemplo: valida se o cliente existe chamando o cliente-service.
     * Se você não tiver endpoint ainda, pode deixar Mono.empty().
     */
    private Mono<Void> validateClienteExternally(Long clienteId) {
        // Exemplo (troque pela URL/rota real):
        // return webClient.get()
        //         .uri("http://cliente-service/clientes/{id}", clienteId)
        //         .retrieve()
        //         .bodyToMono(Void.class);

        return Mono.empty();
    }
}