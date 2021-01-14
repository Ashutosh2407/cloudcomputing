drop table Graph;

create table Graph(
	node bigint,
	neighbour bigint)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Graph;

select neighbour, count (neighbour) as c from Graph group by neighbour order by c desc;
