Final_Iteration = load 's3://data-set/BigdataProject/pageRank_Output';
extract = foreach Final_Iteration generate $0 as university_name, (double)$4 as Page_Rank;
extract_university = FILTER extract by (university_name matches 'university of.*') OR (university_name matches '.*university');
Order_university = order extract_university by $1 DESC;
Top_10 = LIMIT Order_university 100;

store Top_10 into 's3://data-set/BigdataProject/Top_University';
