package dev.wxesquevixos.tcc.reservaservice.kafka.dto;

public final class ReservaEventTypes {
    private ReservaEventTypes() {}

    public static final String PENDING_VALIDATION = "PENDING_VALIDATION";
    public static final String RESERVA_CRIADA = "RESERVA_CRIADA";
    public static final String RESERVA_CONFIRMADA = "RESERVA_CONFIRMADA";
    public static final String RESERVA_CANCELADA = "RESERVA_CANCELADA";
}