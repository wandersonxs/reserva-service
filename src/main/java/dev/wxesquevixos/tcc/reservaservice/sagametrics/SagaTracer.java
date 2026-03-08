package dev.wxesquevixos.tcc.reservaservice.sagametrics;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class SagaTracer {

    private final Tracer tracer;

    public SagaTracer(Tracer tracer) {
        this.tracer = tracer;
    }

    public <T> Mono<T> inSagaStep(String sagaId, String step, Mono<T> pipeline) {
        return Mono.defer(() -> {
            Span span = tracer.nextSpan()
                    .name("saga.step." + step)
                    .tag("saga.id", sagaId)
                    .tag("saga.step", step)
                    .start();

            return pipeline
                    .doOnError(span::error)
                    .doFinally(sig -> span.end())
                    .contextWrite(ctx -> ctx.put(Span.class, span));
        });
    }
}