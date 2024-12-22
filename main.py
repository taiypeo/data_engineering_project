import os

import pandas as pd
import psycopg2

# Establish connection
conn = psycopg2.connect(
    database="<DATABASE>",
    host="<HOST>",
    user="<USER>",
    password="<PASSWORD>",
    port="<PORT>",
)
conn.autocommit = False
cursor = conn.cursor()

# Clear staging
cursor.execute(
    "TRUNCATE ilys_stg_clients, ilys_stg_accounts, ilys_stg_terminals, "
    + "ilys_stg_cards, ilys_stg_transactions, ilys_stg_passport_blacklist CASCADE;"
)
conn.commit()

# Insert new data into staging
passport_blacklist_file = sorted([file for file in os.listdir() if "passport_blacklist_" in file])[0]
terminals_file = sorted([file for file in os.listdir() if "terminals_" in file])[0]
transactions_file = sorted([file for file in os.listdir() if "transactions_" in file])[0]

passport_blacklist = pd.read_excel(passport_blacklist_file)
terminals = pd.read_excel(terminals_file)
transactions = pd.read_csv(transactions_file, sep=";")
transactions["amount"] = transactions["amount"].str.split(",").map(lambda x: float(x[0]) + float(x[1]) / 10 ** len(x[1]))

cursor.execute(
    """
    INSERT INTO ilys_stg_clients (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone)
    SELECT
        client_id,
        last_name,
        first_name,
        patronymic as patrinymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone
    FROM
        info.clients;
    """
)
cursor.execute(
    """
    INSERT INTO ilys_stg_accounts (account_num, valid_to, client)
    SELECT
        account AS account_num,
        valid_to,
        client
    FROM
        info.accounts;
    """
)
cursor.execute(
    """
    INSERT INTO ilys_stg_cards (card_num, account_num)
    SELECT
        card_num,
        account AS account_num
    FROM
        info.cards;
    """
)
cursor.executemany(
    "INSERT INTO ilys_stg_passport_blacklist (entry_dt, passport_num) VALUES (%s, %s)",
    passport_blacklist.values.tolist(),
)
cursor.executemany(
    "INSERT INTO ilys_stg_terminals (terminal_id, terminal_type, terminal_city, terminal_address) VALUES (%s, %s, %s, %s)",
    terminals.values.tolist(),
)
cursor.executemany(
    "INSERT INTO ilys_stg_transactions (trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal) VALUES (%s, %s, %s, %s, %s, %s, %s)",
    transactions.values.tolist(),
)
conn.commit()

# Update facts and dimensions from staging
cursor.execute(
    """
    INSERT INTO ilys_dwh_dim_clients (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone)
    SELECT
        client_id,
        last_name,
        first_name,
        patrinymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone
    FROM
        ilys_stg_clients
    ON CONFLICT (client_id) DO UPDATE
        SET last_name = EXCLUDED.last_name,
            first_name = EXCLUDED.first_name,
            patrinymic = EXCLUDED.patrinymic,
            date_of_birth = EXCLUDED.date_of_birth,
            passport_num = EXCLUDED.passport_num,
            passport_valid_to = EXCLUDED.passport_valid_to,
            phone = EXCLUDED.phone;
    """
)
cursor.execute(
    """
    INSERT INTO ilys_dwh_dim_accounts (account_num, valid_to, client)
    SELECT
        account_num,
        valid_to,
        client
    FROM
        ilys_stg_accounts
    ON CONFLICT (account_num) DO UPDATE
        SET valid_to = EXCLUDED.valid_to,
            client = EXCLUDED.client;
    """
)
cursor.execute(
    """
    INSERT INTO ilys_dwh_dim_cards (card_num, account_num)
    SELECT
        card_num,
        account_num
    FROM
        ilys_stg_cards
    ON CONFLICT (card_num) DO UPDATE
        SET account_num = EXCLUDED.account_num;
    """
)
cursor.execute(
    """
    INSERT INTO ilys_dwh_fact_passport_blacklist (entry_dt, passport_num)
    SELECT
        entry_dt,
        passport_num
    FROM
        ilys_stg_passport_blacklist
    ON CONFLICT (passport_num) DO UPDATE
        SET entry_dt = EXCLUDED.entry_dt;
    """
)
cursor.execute(
    """
    INSERT INTO ilys_dwh_dim_terminals (terminal_id, terminal_type, terminal_city, terminal_address)
    SELECT
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
    FROM
        ilys_stg_terminals
    ON CONFLICT (terminal_id) DO UPDATE
        SET terminal_type = EXCLUDED.terminal_type,
            terminal_city = EXCLUDED.terminal_city,
            terminal_address = EXCLUDED.terminal_address;
    """
)
cursor.execute(
    """
    INSERT INTO ilys_dwh_fact_transactions (trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal)
    SELECT
        trans_id,
        trans_date,
        amt,
        card_num,
        oper_type,
        oper_result,
        terminal
    FROM
        ilys_stg_transactions
    ON CONFLICT (trans_id) DO UPDATE
        SET trans_date = EXCLUDED.trans_date,
            amt = EXCLUDED.amt,
            card_num = EXCLUDED.card_num,
            oper_type = EXCLUDED.oper_type,
            oper_result = EXCLUDED.oper_result,
            terminal = EXCLUDED.terminal;
    """
)
conn.commit()

# Find fraudulent transactions
cursor.execute(
    """
    WITH intermediate_tbl AS (
        SELECT
            ilys_dwh_fact_transactions.trans_date AS event_dt,
            ilys_dwh_dim_clients.passport_num AS passport,
            CONCAT_WS(' ', ilys_dwh_dim_clients.last_name, ilys_dwh_dim_clients.first_name, ilys_dwh_dim_clients.patrinymic) AS fio,
            ilys_dwh_dim_clients.phone AS phone,
            coalesce(ilys_dwh_fact_transactions.trans_date > ilys_dwh_dim_clients.passport_valid_to, false) AS is_passport_invalid_now,
            ilys_stg_passport_blacklist.entry_dt IS NOT NULL AS is_blocked_passport,
            coalesce(ilys_dwh_fact_transactions.trans_date > ilys_dwh_dim_accounts.valid_to, false) AS is_account_invalid_now,
            NOW() AS report_dt
        FROM
            ilys_dwh_fact_transactions
        JOIN
            ilys_dwh_dim_cards ON ilys_dwh_fact_transactions.card_num = ilys_dwh_dim_cards.card_num
        JOIN
            ilys_dwh_dim_accounts ON ilys_dwh_dim_cards.account_num = ilys_dwh_dim_accounts.account_num
        JOIN
            ilys_dwh_dim_clients ON ilys_dwh_dim_accounts.client = ilys_dwh_dim_clients.client_id
        JOIN
            ilys_dwh_dim_terminals ON ilys_dwh_fact_transactions.terminal = ilys_dwh_dim_terminals.terminal_id
        LEFT JOIN
            ilys_stg_passport_blacklist ON ilys_dwh_dim_clients.passport_num = ilys_stg_passport_blacklist.passport_num
    )
    INSERT INTO ilys_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
    SELECT
        event_dt,
        passport,
        fio,
        phone,
        CASE WHEN is_passport_invalid_now THEN 'просроченный паспорт'
             WHEN is_blocked_passport THEN 'заблокированный паспорт'
             ELSE 'просроченный аккаунт'
        END AS event_type,
        report_dt
    FROM intermediate_tbl
    WHERE
        is_passport_invalid_now OR
        is_blocked_passport OR
        is_account_invalid_now;
    """
)

cursor.execute(
    """
    WITH intermediate_tbl AS (
        SELECT
            ilys_dwh_fact_transactions.trans_date AS event_dt,
            ilys_dwh_dim_clients.passport_num AS passport,
            CONCAT_WS(' ', ilys_dwh_dim_clients.last_name, ilys_dwh_dim_clients.first_name, ilys_dwh_dim_clients.patrinymic) AS fio,
            ilys_dwh_dim_clients.phone AS phone,
            ilys_dwh_dim_terminals.terminal_city as terminal_city,
            ilys_dwh_dim_clients.client_id,
            NOW() AS report_dt
        FROM
            ilys_dwh_fact_transactions
        JOIN
            ilys_dwh_dim_cards ON ilys_dwh_fact_transactions.card_num = ilys_dwh_dim_cards.card_num
        JOIN
            ilys_dwh_dim_accounts ON ilys_dwh_dim_cards.account_num = ilys_dwh_dim_accounts.account_num
        JOIN
            ilys_dwh_dim_clients ON ilys_dwh_dim_accounts.client = ilys_dwh_dim_clients.client_id
        JOIN
            ilys_dwh_dim_terminals ON ilys_dwh_fact_transactions.terminal = ilys_dwh_dim_terminals.terminal_id
        LEFT JOIN
            ilys_stg_passport_blacklist ON ilys_dwh_dim_clients.passport_num = ilys_stg_passport_blacklist.passport_num
    )
    INSERT INTO ilys_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
    SELECT
        t1.event_dt,
        t1.passport,
        t1.fio,
        t1.phone,
        'совершение операций в разных городах за короткое время' AS event_type,
        NOW() AS report_dt
    FROM
        intermediate_tbl t1
    JOIN
        intermediate_tbl t2
        ON t1.event_dt < t2.event_dt
        AND t2.event_dt <= t1.event_dt + INTERVAL '1 hour'
        AND t1.terminal_city <> t2.terminal_city
        AND t1.client_id = t2.client_id;
    """
)
conn.commit()

cursor.close()
conn.close()

os.rename(passport_blacklist_file, "archive/" + passport_blacklist_file + ".backup")
os.rename(terminals_file, "archive/" + terminals_file + ".backup")
os.rename(transactions_file, "archive/" + transactions_file + ".backup")
