package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

public record ClienteSnapshot(
        String paymentToken,
        String email,
        String nome
) {}