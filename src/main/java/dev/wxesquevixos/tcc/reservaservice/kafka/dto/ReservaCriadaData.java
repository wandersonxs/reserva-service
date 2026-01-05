package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record ReservaCriadaData(
        Long reservaId,
        Long vooId,
        BigDecimal valor,
        String moeda,
        String metodo,
        String destinatario,
        UUID correlationId
) {}