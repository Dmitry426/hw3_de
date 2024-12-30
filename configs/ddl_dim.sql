CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS dim_terminals (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),  -- UUID as primary key
    terminal_id VARCHAR(20) NOT NULL UNIQUE,
    terminal_type VARCHAR(20) NOT NULL,
    terminal_city VARCHAR(50) NOT NULL,
    terminal_address VARCHAR(255) NOT NULL,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '2300-12-31',
    deleted_flg BOOLEAN DEFAULT FALSE,
    UNIQUE (terminal_id, effective_to)
);

CREATE TABLE IF NOT EXISTS dim_clients (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),  -- UUID as primary key
    client_id VARCHAR(10) NOT NULL UNIQUE,
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE,
    passport_num VARCHAR(15),
    passport_valid_to DATE,
    phone VARCHAR(16),
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '2300-12-31',
    deleted_flg BOOLEAN DEFAULT FALSE,
    UNIQUE (client_id, effective_to)
);

CREATE TABLE IF NOT EXISTS dim_accounts (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),  -- UUID as primary key
    account VARCHAR(50) NOT NULL UNIQUE,
    valid_to DATE,
    client VARCHAR(10) NOT NULL REFERENCES dim_clients(client_id),
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '2300-12-31',
    deleted_flg BOOLEAN DEFAULT FALSE,
    UNIQUE (account, effective_to)
);

CREATE TABLE IF NOT EXISTS dim_cards (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),  -- UUID as primary key
    card_num VARCHAR(50) NOT NULL UNIQUE,
    account VARCHAR(50) NOT NULL REFERENCES dim_accounts(account),
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '2300-12-31',
    deleted_flg BOOLEAN DEFAULT FALSE,
    UNIQUE (card_num, effective_to)
);