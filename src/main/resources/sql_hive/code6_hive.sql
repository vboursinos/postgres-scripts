   create table if not exists tab_t_5_c_1
   row format delimited fields terminated by ',' null defined as '' as
   select a.*, row_number() over (partition by col_o_i order by col_s_c_c desc) as row_no, count(*) over(partition by col_o_i) as col_t_c
   from ${hiveconf:db}.top_5percent_customers_consolidated_1 as a;