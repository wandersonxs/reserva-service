package dev.wxesquevixos.tcc.reservaservice.sagametrics;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.stereotype.Component;

@Component
public class SagaObservations {

    private final ObservationRegistry registry;

    public SagaObservations(ObservationRegistry registry) {
        this.registry = registry;
    }

    public <T> T observeSaga(String sagaId, String flow, java.util.concurrent.Callable<T> action) {
        Observation obs = Observation.start("saga.transaction", registry)
                .lowCardinalityKeyValue("saga.flow", flow)
                .lowCardinalityKeyValue("service", "reserva-service")
                // cuidado: sagaId é alto-cardinality; não use como low-cardinality tag
                .highCardinalityKeyValue("saga.id", sagaId);

        try (Observation.Scope scope = obs.openScope()) {
            T result = action.call();
            obs.lowCardinalityKeyValue("result", "success");
            return result;
        } catch (Exception e) {
            obs.lowCardinalityKeyValue("result", "error");
            obs.error(e);
            throw new RuntimeException(e);
        } finally {
            obs.stop();
        }
    }
}