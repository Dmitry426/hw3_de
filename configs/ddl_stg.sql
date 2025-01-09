CREATE TABLE IF NOT EXISTS dvde_stg_accounts (
    account_id VARCHAR(20) PRIMARY KEY,
    valid_to DATE,
    client_id TEXT,
    create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dvde_stg_passport_blacklist (
    date DATE NOT NULL,
    passport VARCHAR(50) NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dvde_stg_cards (
    card_num VARCHAR(20) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL REFERENCES dvde_stg_accounts(account_id),
    create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dvde_stg_clients (
    client_id VARCHAR(10) PRIMARY KEY,
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE,
    passport_num VARCHAR(15) UNIQUE,
    passport_valid_to DATE,
    phone VARCHAR(16),
    create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dvde_stg_terminals (
    terminal_id VARCHAR(20) PRIMARY KEY,
    terminal_type VARCHAR(20) NOT NULL,
    terminal_city VARCHAR(50) NOT NULL,
    terminal_address VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dvde_stg_transactions (
    transaction_id BIGINT PRIMARY KEY,
    transaction_date TIMESTAMP NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    card_num VARCHAR(20) NOT NULL ,
    oper_type VARCHAR(20) NOT NULL,
    oper_result VARCHAR(20) NOT NULL,
    terminal_id VARCHAR(20) NOT NULL
);