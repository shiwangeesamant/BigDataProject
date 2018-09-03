page= load 's3://data-set/BigdataProject/wikilinkOut' using PigStorage('\t') as (page_title:chararray, page_link:chararray);

new_page = foreach page generate page_title as page_title1, page_link as page_link1;

page_join= join page by page_title, new_page by page_link1 ;

page_generate = foreach page_join generate $0, $1;

page_distinct = distinct page_generate;

store page_distinct into 's3://data-set/BigdataProject/output';
