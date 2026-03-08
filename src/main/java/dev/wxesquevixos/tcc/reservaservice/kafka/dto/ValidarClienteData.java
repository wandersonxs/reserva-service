package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

public record ValidarClienteData(
        Long reservaId,
        Long clienteId
) {}