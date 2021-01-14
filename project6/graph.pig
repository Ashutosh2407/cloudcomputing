G = LOAD '$G' USING PigStorage(',') AS ( id:int, neighbour:int );
B = GROUP G BY neighbour;
RES = FOREACH B GENERATE group,COUNT(G);
Z= ORDER RES BY $1 DESC;
STORE Z INTO '$O' USING PigStorage(',');

