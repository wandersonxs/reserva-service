package dev.wxesquevixos.tcc.reservaservice.sagametrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class SagaMetrics {

    private final MeterRegistry registry;

    public SagaMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public Timer sagaDuration(String flow, String result) {
        return Timer.builder("saga_transaction_duration_seconds")
                .tag("flow", flow)
                .tag("result", result)
                .register(registry);
    }

    public Timer stepDuration(String flow, String step, String result) {
        return Timer.builder("saga_step_duration_seconds")
                .tag("flow", flow)
                .tag("step", step)
                .tag("result", result)
                .register(registry);
    }

    public Counter compensation(String flow, String step, String result) {
        return Counter.builder("saga_compensation_total")
                .tag("flow", flow)
                .tag("step", step)
                .tag("result", result) // success|error
                .register(registry);
    }
}