DROP SCHEMA IF EXISTS demo CASCADE;

CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE demo.tab_c_gt (
    col_s_s_1 BIGINT,
    col_se_s_1 BIGINT,
    col_s_t_d DATE,
    col_s_c_d_i CHAR(1),
    col_s_t_u_a NUMERIC
);
--  \COPY test.tab_c_gt FROM '/home/vasilis/Downloads/Turing_tech/Files_for_turing_tech/Databases/tab_c_gt.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE demo.tab_oab (
    col_o_i NUMERIC,
    col_s_1 BIGINT,
    col_m_p_i VARCHAR(50),
    col_s_s_dt DATE,
    col_s_cn_dt DATE
);

-- \COPY test.tab_c_gt FROM '/home/vasilis/Downloads/Turing_tech/Files_for_turing_tech/Databases/tab_oab.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE demo.tab_t_5_c_c_1 (
    col_o_i NUMERIC,
    col_c_dig_11 NUMERIC,
    col_s_c_c NUMERIC
);

-- \COPY test.tab_c_gt FROM '/home/vasilis/Downloads/Turing_tech/Files_for_turing_tech/Databases/tab_t_5_c_c_1.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE demo.tab_sbfa (
    col_o_i FLOAT,
    col_s_1 BIGINT,
    col_m_p_i VARCHAR(50),
    col_s_s_dt DATE,
    col_s_cn_dt DATE
);

-- \COPY test.tab_sbfa FROM '/home/vasilis/Downloads/Turing_tech/Files_for_turing_tech/Databases/tab_c_gt.csv' DELIMITER ',' CSV HEADER;
