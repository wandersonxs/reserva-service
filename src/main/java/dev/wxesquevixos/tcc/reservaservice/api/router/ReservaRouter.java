package dev.wxesquevixos.tcc.reservaservice.api.router;

import dev.wxesquevixos.tcc.reservaservice.api.handler.ReservaHandler;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaCreateRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.request.ReservaUpdateRequest;
import dev.wxesquevixos.tcc.reservaservice.dtos.response.ReservaResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springdoc.core.annotations.RouterOperation;
import org.springdoc.core.annotations.RouterOperations;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

@Configuration
public class ReservaRouter {

    @Bean
    @RouterOperations({
            @RouterOperation(
                    path = "/api/v1/reservas",
                    method = RequestMethod.POST,
                    beanClass = ReservaHandler.class,
                    beanMethod = "create",
                    operation = @Operation(
                            summary = "Criar reserva",
                            operationId = "criarReserva",
                            tags = {"Reserva"},
                            requestBody = @RequestBody(
                                    required = true,
                                    content = @Content(schema = @Schema(implementation = ReservaCreateRequest.class))
                            ),
                            responses = {
                                    @ApiResponse(responseCode = "201", description = "Reserva criada com sucesso",
                                            content = @Content(schema = @Schema(implementation = ReservaResponse.class))),
                                    @ApiResponse(responseCode = "400", description = "Dados inválidos"),
                                    @ApiResponse(responseCode = "409", description = "Conflito (ex.: correlationId duplicado)")
                            }
                    )
            ),
            @RouterOperation(
                    path = "/api/v1/reservas",
                    method = RequestMethod.GET,
                    beanClass = ReservaHandler.class,
                    beanMethod = "findAll",
                    operation = @Operation(
                            summary = "Listar reservas",
                            operationId = "listarReservas",
                            tags = {"Reserva"},
                            responses = @ApiResponse(
                                    responseCode = "200",
                                    description = "Lista de reservas",
                                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = ReservaResponse.class)))
                            )
                    )
            ),
            @RouterOperation(
                    path = "/api/v1/reservas/{id}",
                    method = RequestMethod.GET,
                    beanClass = ReservaHandler.class,
                    beanMethod = "findById",
                    operation = @Operation(
                            summary = "Buscar reserva por ID",
                            operationId = "buscarReservaPorId",
                            tags = {"Reserva"},
                            parameters = @Parameter(
                                    name = "id",
                                    in = ParameterIn.PATH,
                                    required = true,
                                    schema = @Schema(type = "integer", format = "int64")
                            ),
                            responses = {
                                    @ApiResponse(responseCode = "200", description = "Reserva encontrada",
                                            content = @Content(schema = @Schema(implementation = ReservaResponse.class))),
                                    @ApiResponse(responseCode = "404", description = "Reserva não encontrada")
                            }
                    )
            ),
            @RouterOperation(
                    path = "/api/v1/reservas/{id}",
                    method = RequestMethod.PUT,
                    beanClass = ReservaHandler.class,
                    beanMethod = "update",
                    operation = @Operation(
                            summary = "Atualizar reserva",
                            operationId = "atualizarReserva",
                            tags = {"Reserva"},
                            parameters = @Parameter(
                                    name = "id",
                                    in = ParameterIn.PATH,
                                    required = true,
                                    schema = @Schema(type = "integer", format = "int64")
                            ),
                            requestBody = @RequestBody(
                                    required = true,
                                    content = @Content(schema = @Schema(implementation = ReservaUpdateRequest.class))
                            ),
                            responses = {
                                    @ApiResponse(responseCode = "200", description = "Reserva atualizada",
                                            content = @Content(schema = @Schema(implementation = ReservaResponse.class))),
                                    @ApiResponse(responseCode = "404", description = "Reserva não encontrada")
                            }
                    )
            ),
            @RouterOperation(
                    path = "/api/v1/reservas/{id}",
                    method = RequestMethod.DELETE,
                    beanClass = ReservaHandler.class,
                    beanMethod = "delete",
                    operation = @Operation(
                            summary = "Remover reserva",
                            operationId = "removerReserva",
                            tags = {"Reserva"},
                            parameters = @Parameter(
                                    name = "id",
                                    in = ParameterIn.PATH,
                                    required = true,
                                    schema = @Schema(type = "integer", format = "int64")
                            ),
                            responses = {
                                    @ApiResponse(responseCode = "200", description = "Reserva removida com sucesso"),
                                    @ApiResponse(responseCode = "404", description = "Reserva não encontrada")
                            }
                    )
            )
    })
    public RouterFunction<ServerResponse> reservaRoutes(ReservaHandler handler) {
        return RouterFunctions.route()
                .path("/api/v1/reservas", builder -> builder
                        .POST("", accept(MediaType.APPLICATION_JSON), handler::create)
                        .GET("", handler::findAll)
                        .GET("/{id}", handler::findById)
                        .PUT("/{id}", accept(MediaType.APPLICATION_JSON), handler::update)
                        .DELETE("/{id}", handler::delete)
                )
                .build();
    }
}