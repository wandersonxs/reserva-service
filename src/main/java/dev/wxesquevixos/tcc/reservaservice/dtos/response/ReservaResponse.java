package dev.wxesquevixos.tcc.reservaservice.dtos.response;

import dev.wxesquevixos.tcc.reservaservice.domain.ReservaStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record ReservaResponse(
        Long id,
        Long clienteId,
        Long vooId,
        ReservaStatus status,
        BigDecimal valorTotal,
        String moeda,
        String metodo,
        String motivoCancelamento,
        UUID correlationId,
        OffsetDateTime criadoEm,
        OffsetDateTime atualizadoEm,

        // 🔹 snapshot do cliente
        String paymentToken,
        String email,
        String nome
) {}