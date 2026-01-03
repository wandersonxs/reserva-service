package dev.wxesquevixos.tcc.reservaservice.service;

import dev.wxesquevixos.tcc.reservaservice.domain.ReservaEntity;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaCreateRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaUpdateRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface ReservaService {

    Mono<ReservaEntity> create(ReservaCreateRequest req);

    Mono<ReservaEntity> findById(Long id);

    Mono<ReservaEntity> findByCorrelationId(UUID correlationId);

    Flux<ReservaEntity> findAll();

    Mono<ReservaEntity> update(Long id, ReservaUpdateRequest req);

    Mono<Void> delete(Long id);
}