package dev.wxesquevixos.tcc.reservaservice.dtos.request;

import dev.wxesquevixos.tcc.reservaservice.domain.ReservaStatus;

import java.math.BigDecimal;

public record ReservaUpdateRequest(
        Long clienteId,
        ReservaStatus status,
        BigDecimal valorTotal,
        String moeda
) {}