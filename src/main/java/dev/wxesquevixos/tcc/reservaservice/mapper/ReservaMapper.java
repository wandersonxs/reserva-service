package dev.wxesquevixos.tcc.reservaservice.mapper;

import dev.wxesquevixos.tcc.reservaservice.domain.ReservaEntity;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaCreateRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.response.ReservaResponse;

public final class ReservaMapper {

    private ReservaMapper() {}

    public static ReservaEntity toEntity(ReservaCreateRequest req) {
        return new ReservaEntity(
                null,
                req.clienteId(),
                req.status(),
                req.valorTotal(),
                req.moeda(),
                req.correlationId(),
                null,
                null
        );
    }

    public static ReservaResponse toResponse(ReservaEntity entity) {
        return new ReservaResponse(
                entity.id(),
                entity.clienteId(),
                entity.status(),
                entity.valorTotal(),
                entity.moeda(),
                entity.correlationId(),
                entity.criadoEm(),
                entity.atualizadoEm()
        );
    }
}