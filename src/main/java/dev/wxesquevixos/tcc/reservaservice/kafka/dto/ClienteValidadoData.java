package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

import java.util.UUID;

public record ClienteValidadoData(
        Long reservaId,
        Long clienteId,
        boolean valido,
        String motivo,
        ClienteSnapshot snapshot,
        UUID correlationId
) {}