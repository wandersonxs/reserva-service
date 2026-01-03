package dev.wxesquevixos.tcc.reservaservice.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Table("reserva")
public record ReservaEntity(

        @Id
        Long id,

        @Column("cliente_id")
        Long clienteId,

        @Column("status")
        ReservaStatus status,

        @Column("valor_total")
        BigDecimal valorTotal,

        @Column("moeda")
        String moeda,

        @Column("correlation_id")
        UUID correlationId,

        @Column("criado_em")
        OffsetDateTime criadoEm,

        @Column("atualizado_em")
        OffsetDateTime atualizadoEm
) {
}