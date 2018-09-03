page_distinct = load 's3://data-set/BigdataProject/output' using PigStorage('\t') as (page_title: chararray, page_link: chararray);

page_grp = group page1 by $0;
OUTgoingLink= foreach page_grp generate group, COUNT($1);

page_grp_all = group page_grp ALL;
page_count = foreach page_grp_all generate group, (double)COUNT($1) AS Total_pages; 

in_out_links = join OUTgoingLink by $0, page1 by $0;
page = foreach in_out_links generate $2 as pi, $3 as pj, $1 as No_outgoing, 1 as no;
final_page= CROSS page_count, page;
generate_pages = foreach final_page generate $2 as pi, $3 as pj, $4 as No_outgoing, $1 as Total_nodes, $5/$1 as PR;

generate_pages_pr = foreach generate_pages generate pi, pj, No_outgoing, Total_nodes, (1-0.85)/Total_nodes as first_of_algorithm, (double)PR/No_outgoing as div_PR;
group_pj= group generate_pages_pr by pj;
Sum_of_group_pj = foreach group_pj generate group, $1 as data, 0.85*SUM($1.$5) as second_of_algorithm;
flatten_Sum = foreach Sum_of_group_pj generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_1 = foreach flatten_Sum generate pj, $1 as pi, $3 as No_outgoing, $5 as first_of_algorithm, (double)$5+second_of_algorithm as Page_Rank1;

page_rank_gen = foreach Iteration_1 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank1;
filter_page_rank_gen= foreach page_rank_gen generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank = DISTINCT filter_page_rank_gen;
join_page= join distinct_filter_page_rank by $0, page_rank_gen by $0;
join_page_gen = foreach join_page generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_1 = foreach join_page_gen generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr1 = foreach generate_pages_1 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR1;
group_pj1= group generate_pages_pr1 by pj;
Sum_of_group_pj1 = foreach group_pj1 generate group, $1 as data1, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum1 = foreach Sum_of_group_pj1 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_2= foreach flatten_Sum1 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank2 ;

page_rank_gen1 = foreach Iteration_2 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank2;
filter_page_rank_gen1= foreach page_rank_gen1 generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank1 = DISTINCT filter_page_rank_gen1;
join_page1= join distinct_filter_page_rank1 by $0, page_rank_gen1 by $0;
join_page_gen1 = foreach join_page1 generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_2 = foreach join_page_gen1 generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr2 = foreach generate_pages_2 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR2;
group_pj2= group generate_pages_pr2 by pj;
Sum_of_group_pj2 = foreach group_pj2 generate group, $1 as data2, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum2 = foreach Sum_of_group_pj2 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_3 = foreach flatten_Sum2 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank3;

page_rank_gen2 = foreach Iteration_3 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank3;
filter_page_rank_gen2= foreach page_rank_gen2 generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank2 = DISTINCT filter_page_rank_gen2;
join_page2= join distinct_filter_page_rank2 by $0, page_rank_gen2 by $0;
join_page_gen2 = foreach join_page2 generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_3 = foreach join_page_gen2 generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr3 = foreach generate_pages_3 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR3;
group_pj3= group generate_pages_pr3 by pj;
Sum_of_group_pj3 = foreach group_pj3 generate group, $1 as data3, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum3 = foreach Sum_of_group_pj3 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_4 = foreach flatten_Sum3 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank4;

page_rank_gen3 = foreach Iteration_4 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank4;
filter_page_rank_gen3= foreach page_rank_gen3 generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank3 = DISTINCT filter_page_rank_gen3;
join_page3= join distinct_filter_page_rank3 by $0, page_rank_gen3 by $0;
join_page_gen3 = foreach join_page3 generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_4 = foreach join_page_gen3 generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr4 = foreach generate_pages_4 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR4;
group_pj4= group generate_pages_pr4 by pj;
Sum_of_group_pj4 = foreach group_pj4 generate group, $1 as data4, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum4 = foreach Sum_of_group_pj4 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_5 = foreach flatten_Sum4 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank5;

page_rank_gen4 = foreach Iteration_5 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank5;
filter_page_rank_gen4= foreach page_rank_gen4 generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank4 = DISTINCT filter_page_rank_gen4;
join_page4= join distinct_filter_page_rank4 by $0, page_rank_gen4 by $0;
join_page_gen4 = foreach join_page4 generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_5 = foreach join_page_gen4 generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr5 = foreach generate_pages_5 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR5;
group_pj5= group generate_pages_pr5 by pj;
Sum_of_group_pj5 = foreach group_pj5 generate group, $1 as data5, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum5 = foreach Sum_of_group_pj5 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_6 = foreach flatten_Sum5 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank6;

page_rank_gen5 = foreach Iteration_6 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank6;
filter_page_rank_gen5= foreach page_rank_gen5 generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank5 = DISTINCT filter_page_rank_gen5;
join_page5= join distinct_filter_page_rank5 by $0, page_rank_gen5 by $0;
join_page_gen5 = foreach join_page5 generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_6 = foreach join_page_gen5 generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr6 = foreach generate_pages_6 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR6;
group_pj6= group generate_pages_pr6 by pj;
Sum_of_group_pj6 = foreach group_pj6 generate group, $1 as data6, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum6 = foreach Sum_of_group_pj6 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_7 = foreach flatten_Sum6 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank7;

page_rank_gen6 = foreach Iteration_7 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank7;
filter_page_rank_gen6= foreach page_rank_gen6 generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank6 = DISTINCT filter_page_rank_gen6;
join_page6= join distinct_filter_page_rank6 by $0, page_rank_gen6 by $0;
join_page_gen6 = foreach join_page6 generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_7 = foreach join_page_gen6 generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr7 = foreach generate_pages_7 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR7;
group_pj7= group generate_pages_pr7 by pj;
Sum_of_group_pj7 = foreach group_pj7 generate group, $1 as data6, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum7 = foreach Sum_of_group_pj7 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_8 = foreach flatten_Sum7 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank8;

page_rank_gen7 = foreach Iteration_8 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank8;
filter_page_rank_gen7= foreach page_rank_gen7 generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank7 = DISTINCT filter_page_rank_gen7;
join_page7= join distinct_filter_page_rank7 by $0, page_rank_gen7 by $0;
join_page_gen7 = foreach join_page7 generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_8 = foreach join_page_gen7 generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr8 = foreach generate_pages_8 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR8;
group_pj8= group generate_pages_pr8 by pj;
Sum_of_group_pj8 = foreach group_pj8 generate group, $1 as data6, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum8 = foreach Sum_of_group_pj8 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_9 = foreach flatten_Sum8 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank9;

page_rank_gen8 = foreach Iteration_9 generate pi , pj, No_outgoing,first_of_algorithm,Page_Rank9;
filter_page_rank_gen8= foreach page_rank_gen8 generate $1 as pj, $4 as PR_pj;
distinct_filter_page_rank8 = DISTINCT filter_page_rank_gen8;
join_page8= join distinct_filter_page_rank8 by $0, page_rank_gen8 by $0;
join_page_gen8 = foreach join_page8 generate $0 as pi, $3 as pj, $4 as No_outgoing, $5 as first_of_algorithm, $1 as PR_of_pi, $6 as PR_of_pj;

generate_pages_9 = foreach join_page_gen8 generate pi,pj,No_outgoing,first_of_algorithm,PR_of_pi;
generate_pages_pr9 = foreach generate_pages_9 generate pi,pj,No_outgoing,first_of_algorithm,(double)PR_of_pi/No_outgoing as div_PR9;
group_pj9= group generate_pages_pr9 by pj;
Sum_of_group_pj9 = foreach group_pj9 generate group, $1 as data6, 0.85*SUM($1.$4) as second_of_algorithm;
flatten_Sum9 = foreach Sum_of_group_pj9 generate $0 as pj, FLATTEN($1), second_of_algorithm;
Iteration_10 = foreach flatten_Sum9 generate  pi, pj, $3 as No_outgoing, $4 as first_of_algorithm, $4+$6 as Page_Rank10;

store Iteration_10 into 's3://data-set/BigdataProject/pageRank_Output';





