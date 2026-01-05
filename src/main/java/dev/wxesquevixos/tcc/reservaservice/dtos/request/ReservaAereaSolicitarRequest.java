package dev.wxesquevixos.tcc.reservaservice.dtos.request;

import java.math.BigDecimal;
import java.util.UUID;

public record ReservaAereaSolicitarRequest(
        Long clienteId,
        Long vooId,
        BigDecimal valor,
        String moeda,
        String metodo,
        String destinatario,
        UUID correlationId
) {}