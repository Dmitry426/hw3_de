CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS dvde_fact_transactions (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id BIGINT,
    transaction_date TIMESTAMP NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    card_num VARCHAR(50) NOT NULL,
    terminal_id VARCHAR(20) NOT NULL,
    oper_type VARCHAR(20) NOT NULL,
    oper_result VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dvde_fact_passport_blacklist (
	id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
	blacklist_date date NOT NULL,
	passport varchar(15) NOT NULL,
	UNIQUE (passport)
);