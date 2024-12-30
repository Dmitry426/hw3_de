CREATE TABLE IF NOT EXISTS stg_accounts(
    account bpchar(20) NOT NULL,
    valid_to DATE,
    client TEXT,
    create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
    );


CREATE TABLE IF NOT EXISTS  stg_passport_blacklist (
    date DATE NOT NULL,
    passport VARCHAR(50) NOT NULL,
    PRIMARY KEY (passport)
);


CREATE TABLE IF NOT EXISTS stg_cards (
    card_num bpchar(20) NOT NULL,
	account bpchar(20) NOT NULL,
    create_dt timestamp(0),
	update_dt timestamp(0)
);


CREATE TABLE IF NOT EXISTS stg_clients (
	client_id varchar(10)PRIMARY KEY,
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone bpchar(16),
	create_dt timestamp(0),
	update_dt timestamp(0)
);


CREATE TABLE IF NOT EXISTS stg_terminals (
    terminal_id VARCHAR(20) PRIMARY KEY,
    terminal_type VARCHAR(20) NOT NULL,
    terminal_city VARCHAR(50) NOT NULL,
    terminal_address VARCHAR(255) NOT NULL
);


CREATE TABLE IF NOT EXISTS stg_transactions (
    transaction_id BIGINT PRIMARY KEY,
    transaction_date TIMESTAMP NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    card_num VARCHAR(20) NOT NULL,
    oper_type VARCHAR(20) NOT NULL,
    oper_result VARCHAR(20) NOT NULL,
    terminal VARCHAR(20) NOT NULL
);
