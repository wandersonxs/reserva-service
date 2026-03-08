package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

public record ClienteSnapshotData(
        String nome,
        String email,
        String paymentToken
) {}