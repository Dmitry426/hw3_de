CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS dvde_dim_terminals (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    terminal_id VARCHAR(20) NOT NULL ,
    terminal_type VARCHAR(20) NOT NULL,
    terminal_city VARCHAR(50) NOT NULL,
    terminal_address VARCHAR(255) NOT NULL,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '2300-12-31',
    deleted_flg INTEGER DEFAULT 0 CHECK (deleted_flg IN (0, 1)),
    UNIQUE (uuid, effective_to)
);

CREATE TABLE IF NOT EXISTS dvde_dim_clients (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id VARCHAR(10) NOT NULL ,
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE,
    passport_num VARCHAR(15),
    passport_valid_to DATE,
    phone VARCHAR(16),
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '2300-12-31',
    deleted_flg INTEGER DEFAULT 0 CHECK (deleted_flg IN (0, 1)),
    UNIQUE (uuid, effective_to)
);

CREATE TABLE IF NOT EXISTS dvde_dim_accounts (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id VARCHAR(50) NOT NULL ,
    valid_to DATE,
    client_id VARCHAR(10) NOT NULL,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '2300-12-31',
    deleted_flg INTEGER DEFAULT 0 CHECK (deleted_flg IN (0, 1)),
    UNIQUE (uuid, effective_to)
);

CREATE TABLE IF NOT EXISTS dvde_dim_cards (
    uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    card_num VARCHAR(50) NOT NULL ,
    account_id VARCHAR(50) NOT NULL,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '2300-12-31',
    deleted_flg INTEGER DEFAULT 0 CHECK (deleted_flg IN (0, 1)),
    UNIQUE (uuid, effective_to)
);