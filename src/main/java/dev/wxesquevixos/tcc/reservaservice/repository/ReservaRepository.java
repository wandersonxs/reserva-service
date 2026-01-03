package dev.wxesquevixos.tcc.reservaservice.repository;

import dev.wxesquevixos.tcc.reservaservice.domain.ReservaEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import java.util.UUID;
import reactor.core.publisher.Mono;

public interface ReservaRepository extends ReactiveCrudRepository<ReservaEntity, Long> {

    Mono<ReservaEntity> findByCorrelationId(UUID correlationId);

    Mono<Boolean> existsByCorrelationId(UUID correlationId);
}