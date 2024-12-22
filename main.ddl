-- ============== Staging tables ==============

CREATE TABLE ilys_stg_clients (
    client_id VARCHAR PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR
);

CREATE TABLE ilys_stg_accounts (
    account_num VARCHAR PRIMARY KEY,
    valid_to DATE,
    client VARCHAR REFERENCES ilys_stg_clients (client_id)
);

CREATE TABLE ilys_stg_terminals (
    terminal_id VARCHAR PRIMARY KEY,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR
);

CREATE TABLE ilys_stg_cards (
    card_num VARCHAR PRIMARY KEY,
    account_num VARCHAR REFERENCES ilys_stg_accounts (account_num)
);

CREATE TABLE ilys_stg_transactions (
    trans_id VARCHAR,
    trans_date TIMESTAMP,
    card_num VARCHAR REFERENCES ilys_stg_cards (card_num),
    oper_type VARCHAR,
    amt DECIMAL,
    oper_result VARCHAR,
    terminal VARCHAR REFERENCES ilys_stg_terminals (terminal_id)
);

CREATE TABLE ilys_stg_passport_blacklist (
    passport_num VARCHAR,
    entry_dt DATE
);

-- ============== Dimension tables ==============

CREATE TABLE ilys_dwh_dim_clients (
    client_id VARCHAR PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR
);

CREATE TABLE ilys_dwh_dim_accounts (
    account_num VARCHAR PRIMARY KEY,
    valid_to DATE,
    client VARCHAR REFERENCES ilys_dwh_dim_clients (client_id)
);

CREATE TABLE ilys_dwh_dim_cards (
    card_num VARCHAR PRIMARY KEY,
    account_num VARCHAR REFERENCES ilys_dwh_dim_accounts (account_num)
);

CREATE TABLE ilys_dwh_dim_terminals (
    terminal_id VARCHAR PRIMARY KEY,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR
);

-- ============== Fact tables ==============

CREATE TABLE ilys_dwh_fact_transactions (
    trans_id VARCHAR UNIQUE,
    trans_date TIMESTAMP,
    card_num VARCHAR REFERENCES ilys_dwh_dim_cards (card_num),
    oper_type VARCHAR,
    amt DECIMAL,
    oper_result VARCHAR,
    terminal VARCHAR REFERENCES ilys_dwh_dim_terminals (terminal_id)
);

CREATE TABLE ilys_dwh_fact_passport_blacklist (
    passport_num VARCHAR UNIQUE,
    entry_dt DATE
);

CREATE TABLE ilys_rep_fraud (
    event_dt TIMESTAMP,
    passport VARCHAR,
    fio VARCHAR,
    phone VARCHAR,
    event_type VARCHAR,
    report_dt TIMESTAMP
);
