CREATE TABLE IF NOT EXISTS tab_t_a_l_s_b AS
SELECT 
    a.col_s_1,
    a.col_o_i,
    b.col_s_t_d,
    SUM(
        CASE 
            WHEN b.col_s_c_d_i = 'd' THEN b.col_s_t_u_a 
            WHEN b.col_s_c_d_i = 'c' THEN -1 * b.col_s_t_u_a  
            ELSE 0 
        END) AS col_s_c,
    SUM(
        CASE 
            WHEN b.col_s_c_d_i = 'd' THEN 1 
            ELSE 0 
        END) AS col_s_r
FROM 
    test.tab_sbfa as a 
INNER JOIN 
    test.tab_c_gt b
ON 
    a.col_s_1::numeric = b.col_se_s_1::numeric 
--    AND trim(b.col_se_s_1) <> trim(b.col_s_s_1)
        AND TRIM(CAST(b.col_se_s_1 AS TEXT)) <> TRIM(CAST(b.col_s_s_1 AS TEXT))
    AND a.col_m_p_i NOT IN ('PROP')
	AND b.col_s_t_d >= CAST('2022-11-01' AS DATE) - INTERVAL '12 months' 
	AND b.col_s_t_d < CAST('2022-11-01' AS DATE)
GROUP BY 
    a.col_s_1,
    a.col_o_i,
    b.col_s_t_d
HAVING 
    SUM(
        CASE 
            WHEN b.col_s_c_d_i = 'd' THEN b.col_s_t_u_a 
            WHEN b.col_s_c_d_i = 'c' THEN -1 * b.col_s_t_u_a  
            ELSE 0 
        END) > 10 
    AND SUM(
        CASE 
            WHEN b.col_s_c_d_i = 'd' THEN 1 
            ELSE 0 
        END) > 0;	
