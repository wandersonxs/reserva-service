package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

import java.math.BigDecimal;

public record ReservaConfirmadaData(
        Long reservaId,
        Long vooId,
        BigDecimal valor,
        String moeda,
        String metodo,
        ClienteSnapshot snapshot
) {}