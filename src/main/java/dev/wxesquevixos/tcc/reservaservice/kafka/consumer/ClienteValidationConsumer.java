package dev.wxesquevixos.tcc.reservaservice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteValidadoData;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.ClienteValidationEventTypes;
import dev.wxesquevixos.tcc.reservaservice.kafka.dto.EventEnvelope;
import dev.wxesquevixos.tcc.reservaservice.service.ReservaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class ClienteValidationConsumer {

    private static final Logger log = LoggerFactory.getLogger(ClienteValidationConsumer.class);

    private final ObjectMapper objectMapper;
    private final ReservaService reservaService;

    public ClienteValidationConsumer(ObjectMapper objectMapper, ReservaService reservaService) {
        this.objectMapper = objectMapper;
        this.reservaService = reservaService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.cliente-validation}",
            groupId = "${app.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {

        try {
            final EventEnvelope env = objectMapper.convertValue(record.value(), EventEnvelope.class);
            final var correlationId = env.correlationId();

            if (!ClienteValidationEventTypes.CLIENTE_VALIDADO.equals(env.type())) {
                log.info("Ignorando evento em cliente.validation: type={} correlationId={}", env.type(), correlationId);
                ack.acknowledge();
                return;
            }

            // env.data é Map<String,Object> -> converte pra DTO
            final ClienteValidadoData data = objectMapper.convertValue(env.data(), ClienteValidadoData.class);

            Mono<Void> pipeline = reservaService.onClienteValidado(env, data);

            pipeline
                    .doOnSuccess(v -> ack.acknowledge())
                    .doOnError(ex -> {
                        log.error("Erro processando CLIENTE_VALIDADO. topic={} partition={} offset={} key={} correlationId={} msg={}",
                                record.topic(), record.partition(), record.offset(), record.key(),
                                correlationId, ex.getMessage(), ex);
                        ack.acknowledge();
                    })
                    .subscribe();

        } catch (Exception ex) {
            log.error("Erro no consumer cliente.validation (parse/process). topic={} partition={} offset={} key={} msg={}",
                    record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage(), ex);
            ack.acknowledge();
        }
    }
}