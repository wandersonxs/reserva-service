package dev.wxesquevixos.tcc.reservaservice.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI reservaOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Reserva Service API")
                        .version("v1")
                        .description("API responsável por manter dados de reservas"));
    }
}