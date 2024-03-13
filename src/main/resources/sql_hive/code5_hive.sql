set run_dt_start = 2022-11-01;
   create table if not exists tab_t_5_c_s
   row format delimited fields terminated by ',' null defined as '' as
   select col_o_i,substring(col_c_dig,1,11) as col_c_dig_11,sum(case when b.col_s_c_d_i='d' then b.col_s_t_u_a
   when b.col_s_c_d_i='c' then -1*b.col_s_t_u_a
   else 0 end) as col_s_c_c
   from ${hiveconf:db}.tab_sbfa as a
   inner join c_db.tab_c_gt b
   on a.col_s_1*1=b.col_se_s_1*1
   and trim(b.col_se_s_1)<>trim(b.col_s_s_1)
   where a.col_m_p_i not in ("prop")
   and b.col_s_t_d >= add_months(cast('${hiveconf:run_dt_start}' as date),-12) and b.col_s_t_d < cast('${hiveconf:run_dt_start}' as date)
   and b.col_s_i_n_p_i<>'n'
   group by col_o_i,substring(col_c_dig,1,11);