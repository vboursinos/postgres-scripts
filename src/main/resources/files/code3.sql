CREATE TABLE test.tab_tb2_c AS
SELECT * FROM 
(
    SELECT a.*, sum(a.col_s_c) OVER (PARTITION BY a.col_s_1) AS col_s_c_s_1, b.col_o_i, NULL AS extra_col
    FROM test.tab_tb2_1 AS a
    LEFT JOIN test.tab_oab AS b ON a.col_s_1 = b.col_s_1

    UNION 

    SELECT c.*, sum(c.col_s_c) OVER (PARTITION BY c.col_s_1) AS col_s_c_s_1, d.col_o_i 
    FROM test.tab_tb2_2 AS c
    LEFT JOIN test.tab_oab AS d ON c.col_s_1 = d.col_s_1
) AS e WHERE col_s_c_s_1>0;
