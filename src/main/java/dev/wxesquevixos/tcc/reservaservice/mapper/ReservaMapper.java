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
                req.vooId(),
                req.status(),
                req.valorTotal(),
                req.moeda(),
                req.metodo(),
                null,                  // motivoCancelamento
                req.correlationId(),
                null,                  // criadoEm (definido no service)
                null,                  // atualizadoEm (definido no service)
                req.paymentToken(),    // clientePaymentToken
                req.email(),           // clienteEmail
                req.nome()             // clienteNome
        );
    }

    public static ReservaResponse toResponse(ReservaEntity entity) {
        return new ReservaResponse(
                entity.id(),
                entity.clienteId(),
                entity.vooId(),
                entity.status(),
                entity.valorTotal(),
                entity.moeda(),
                entity.metodo(),
                entity.motivoCancelamento(),
                entity.correlationId(),
                entity.criadoEm(),
                entity.atualizadoEm(),
                entity.clientePaymentToken(),
                entity.clienteEmail(),
                entity.clienteNome()
        );
    }
}