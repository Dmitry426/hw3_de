CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id BIGINT PRIMARY KEY,
    transaction_date TIMESTAMP NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    card_num VARCHAR(50) NOT NULL,
    terminal_id VARCHAR(20) NOT NULL,
    oper_type VARCHAR(20) NOT NULL,
    oper_result VARCHAR(20) NOT NULL,
    FOREIGN KEY (card_num) REFERENCES dim_cards (card_num),
    FOREIGN KEY (terminal_id) REFERENCES dim_terminals (terminal_id)
);

CREATE TABLE IF NOT EXISTS public.fact_passport_blacklist (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	blacklist_date date NOT NULL,
	passport varchar(15) NOT NULL,
	CONSTRAINT fact_passport_blacklist_pkey PRIMARY KEY (id)
);