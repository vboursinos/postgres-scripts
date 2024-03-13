set run_dt_start = 2022-11-01;
create table if not exists tab_tb2_2
   row format delimited fields terminated by ',' null defined as '' as
   select col_s_1,col_t_m,max(col_s_t_d) as col_m_t_d,
   sum(case when col_s_c_d_i='d' then col_s_t_u_a
   when col_s_c_d_i='c' then -1*col_s_t_u_a
   else 0 end) as col_s_c,
   sum(case when col_s_c_d_i='d' then col_s_t_u_a else 0 end) as col_d_c,
   sum(case when col_s_c_d_i='c' then col_s_t_u_a else 0 end) as col_c_cv,
   sum(case when col_s_c_d_i='d' then 1 else 0 end) as col_s_r,
   sum(case when col_s_c_d_i='c' then 1 else 0 end) as col_c_r
   from
   (

select a.col_s_1,case when col_s_c_d_i ='d' then col_s_t_d else null end as col_s_t_d ,ceiling(months_between(cast('${hiveconf:run_dt_start}' as date), cast(b.col_s_t_d as date))) as col_t_m,b.col_s_c_d_i,b.col_s_t_u_a
   from ${hiveconf:db}.tab_oab as a
   inner join c_db.tab_c_gt b
   on a.col_s_1*1=b.col_se_s_1*1
   and trim(b.col_se_s_1)<>trim(b.col_s_s_1)
   and a.col_m_p_i not in ("prop")
   and b.col_s_t_d >=add_months(cast('${hiveconf:run_dt_start}' as date),-24) and b.col_s_t_d< cast('${hiveconf:run_dt_start}' as date)
   and b.col_s_i_n_p_i<>'n'

) as c
   group by col_s_1,col_t_m;