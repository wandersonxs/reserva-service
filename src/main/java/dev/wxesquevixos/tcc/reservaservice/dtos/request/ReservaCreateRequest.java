package dev.wxesquevixos.tcc.reservaservice.dtos.request;

import dev.wxesquevixos.tcc.reservaservice.domain.ReservaStatus;

import java.math.BigDecimal;
import java.util.UUID;

public record ReservaCreateRequest(
        Long clienteId,
        ReservaStatus status,      // opcional: se null -> PENDING no service
        BigDecimal valorTotal,
        String moeda,             // opcional: se null/blank -> BRL no service
        UUID correlationId
) {}