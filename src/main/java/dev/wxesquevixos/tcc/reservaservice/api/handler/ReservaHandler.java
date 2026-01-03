package dev.wxesquevixos.tcc.reservaservice.api.handler;

import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaCreateRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaUpdateRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.response.ReservaResponse;
import dev.wxesquevixos.tcc.reservaservice.mapper.ReservaMapper;
import dev.wxesquevixos.tcc.reservaservice.service.ReservaService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.springframework.web.reactive.function.server.ServerResponse.*;

@Component
public class ReservaHandler {

    private final ReservaService service;
    private final Validator validator;

    public ReservaHandler(ReservaService service, Validator validator) {
        this.service = service;
        this.validator = validator;
    }

    public Mono<ServerResponse> create(ServerRequest request) {
        return request.bodyToMono(ReservaCreateRequest.class)
                .flatMap(this::validate)
                .flatMap(service::create)
                .map(ReservaMapper::toResponse)
                .flatMap(resp -> status(201)
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(resp))
                .onErrorResume(this::mapError);
    }

    public Mono<ServerResponse> findById(ServerRequest request) {
        Long id = Long.valueOf(request.pathVariable("id"));
        return service.findById(id)
                .map(ReservaMapper::toResponse)
                .flatMap(resp -> ok().contentType(MediaType.APPLICATION_JSON).bodyValue(resp))
                .onErrorResume(this::mapError);
    }

    public Mono<ServerResponse> findAll(ServerRequest request) {
        return ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(service.findAll().map(ReservaMapper::toResponse), ReservaResponse.class);
    }

    public Mono<ServerResponse> update(ServerRequest request) {
        Long id = Long.valueOf(request.pathVariable("id"));
        return request.bodyToMono(ReservaUpdateRequest.class)
                .flatMap(this::validate)
                .flatMap(req -> service.update(id, req))
                .map(ReservaMapper::toResponse)
                .flatMap(resp -> ok().contentType(MediaType.APPLICATION_JSON).bodyValue(resp))
                .onErrorResume(this::mapError);
    }

    public Mono<ServerResponse> delete(ServerRequest request) {
        Long id = Long.valueOf(request.pathVariable("id"));
        return service.delete(id)
                .then(ok().build())
                .onErrorResume(this::mapError);
    }

    // -------------------------
    // Validation + error mapping
    // -------------------------

    private <T> Mono<T> validate(T body) {
        var violations = validator.validate(body);
        if (violations.isEmpty()) return Mono.just(body);

        String message = violations.stream()
                .map(this::formatViolation)
                .collect(Collectors.joining("; "));

        return Mono.error(new IllegalArgumentException(message));
    }

    private String formatViolation(ConstraintViolation<?> v) {
        return v.getPropertyPath() + ": " + v.getMessage();
    }

    private Mono<ServerResponse> mapError(Throwable ex) {
        // 1. NÃO ENCONTRADO
        if (ex instanceof NoSuchElementException || (ex.getMessage() != null && ex.getMessage().contains("não encontrada"))) {
            return status(404)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(new ErrorResponse("NOT_FOUND", ex.getMessage()));
        }

        // 2. VALIDAÇÃO
        if (ex instanceof IllegalArgumentException) {
            return badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(new ErrorResponse("VALIDATION_ERROR", ex.getMessage()));
        }

        // 3. FORMATO DE PARÂMETRO
        if (ex instanceof NumberFormatException || ex instanceof org.springframework.web.server.ServerWebInputException) {
            return badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(new ErrorResponse("BAD_REQUEST", "Formato de parâmetro inválido."));
        }

        // 4. CONFLITO
        if (ex instanceof IllegalStateException) {
            return status(409)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(new ErrorResponse("CONFLICT", ex.getMessage()));
        }

        // 5. ERRO GENÉRICO
        return status(500)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new ErrorResponse("INTERNAL_ERROR", "Erro inesperado"));
    }

    public record ErrorResponse(String code, String message) {}
}