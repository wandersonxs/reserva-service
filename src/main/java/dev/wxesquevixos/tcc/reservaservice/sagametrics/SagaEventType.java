package dev.wxesquevixos.tcc.reservaservice.sagametrics;

public enum SagaEventType {
    SAGA_STARTED,
    STEP_STARTED,
    STEP_COMPLETED,
    FAIL_DETECTED,
    COMPENSATION_STARTED,
    COMPENSATION_COMPLETED,
    RECOVERED,
    SAGA_COMPLETED
}