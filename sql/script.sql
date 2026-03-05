CREATE TABLE reserva (
                         id BIGSERIAL PRIMARY KEY,

                         cliente_id BIGINT NOT NULL,
                         voo_id BIGINT NOT NULL,

                         status VARCHAR(30) NOT NULL
                             CHECK (status IN (
                                               'PENDING_VALIDATION',
                                               'PENDING',
                                               'CONFIRMED',
                                               'CANCELLED'
                                 )),

                         valor_total NUMERIC(12,2) NOT NULL,
                         moeda VARCHAR(3) NOT NULL,

                         metodo VARCHAR(50),

                         motivo_cancelamento TEXT,

                         correlation_id UUID NOT NULL,

                         criado_em TIMESTAMPTZ NOT NULL,
                         atualizado_em TIMESTAMPTZ,

                         cliente_payment_token VARCHAR(255),
                         cliente_email VARCHAR(150),
                         cliente_nome VARCHAR(150)
);

CREATE INDEX idx_reserva_cliente_id
    ON reserva(cliente_id);

CREATE INDEX idx_reserva_voo_id
    ON reserva(voo_id);

CREATE INDEX idx_reserva_status
    ON reserva(status);

CREATE INDEX idx_reserva_correlation_id
    ON reserva(correlation_id);


INSERT INTO reserva (
    cliente_id,
    voo_id,
    status,
    valor_total,
    moeda,
    metodo,
    correlation_id,
    criado_em,
    atualizado_em,
    cliente_payment_token,
    cliente_email,
    cliente_nome
) VALUES (
             1,
             1001,
             'PENDING',
             1250.00,
             'BRL',
             'CREDIT_CARD',
             '11111111-1111-1111-1111-111111111111',
             NOW(),
             NOW(),
             'tok_visa_9f3a2c1b',
             'joao.silva@email.com',
             'João da Silva'
         );



INSERT INTO reserva (
    cliente_id,
    voo_id,
    status,
    valor_total,
    moeda,
    metodo,
    correlation_id,
    criado_em,
    atualizado_em,
    cliente_payment_token,
    cliente_email,
    cliente_nome,
    motivo_cancelamento
) VALUES (
             2,
             2002,
             'CANCELLED',
             980.50,
             'BRL',
             'PIX',
             '22222222-2222-2222-2222-222222222222',
             NOW(),
             NOW(),
             'tok_master_7d2e4f9a',
             'maria.oliveira@email.com',
             'Maria Oliveira',
             'Falha na autorização de pagamento'
         );


/*

 ALTER TABLE reserva
ADD CONSTRAINT chk_cancel_reason
CHECK (
    (status <> 'CANCELLED')
    OR
    (status = 'CANCELLED' AND motivo_cancelamento IS NOT NULL)
);
 */

CREATE DATABASE saga_experiment;

CREATE SCHEMA IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.saga_run (
                                                      id BIGSERIAL PRIMARY KEY,
                                                      service_name VARCHAR(80) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_name VARCHAR(80) NOT NULL,
    final_status VARCHAR(30) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT,
    error_message TEXT
    );

CREATE INDEX IF NOT EXISTS idx_saga_run_correlation_id
    ON observability.saga_run(correlation_id);