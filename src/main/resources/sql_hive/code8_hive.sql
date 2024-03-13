set run_dt_start = 2022-11-01;
   create table if not exists tab_t_a_l_s_b
   row format delimited fields terminated by ',' null defined as '' as
   select a.col_s_1,a.col_o_i,b.col_s_t_d,
   sum(case when b.col_s_c_d_i='d' then b.col_s_t_u_a
   when b.col_s_c_d_i='c' then -1*b.col_s_t_u_a
   else 0 end) as col_s_c,
   sum(case when b.col_s_c_d_i='d' then 1 else 0 end) as col_s_r
   from ${hiveconf:db}.tab_sbfa as a
   inner join c_db.tab_c_gt b
   on a.col_s_1*1=b.col_se_s_1*1
   and trim(b.col_se_s_1)<>trim(b.col_s_s_1)
   and a.col_m_p_i not in ("prop")
   and b.col_s_t_d>=add_months(cast('${hiveconf:run_dt_start}' as date),-12)and b.col_s_t_d<cast('${hiveconf:run_dt_start}' as date)
   and b.col_s_i_n_p_i<>'n'
   group by a.col_s_1,a.col_o_i,b.col_s_t_d
   having col_s_c>10 and col_s_r>0;