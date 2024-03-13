   create table if not exists ${hiveconf:db}.tab_tb2_c
   row format delimited fields terminated by ',' null defined as '' as
   select * from
   (
   select a.*,sum(a.col_s_c)over(partition by a.col_s_1) as col_s_c_s_1, b.col_o_i
   from ${hiveconf:db}.tab_tb2_1 as a
   left join ${hiveconf:db}.tab_oab as b
   on a.col_s_1 = b.col_s_1
   union
   select c.*,sum(c.col_s_c)over(partition by c.col_s_1) as col_s_c_s_1, d.col_o_i
   from ${hiveconf:db}.tab_tb2_2 as c
   left join ${hiveconf:db}.tab_oab as d
   on c.col_s_1 = d.col_s_1
   ) as e
   where col_s_c_s_1>0;