create database sql_analyzer;


create schema api_data;


CREATE USER sql_analyzer WITH PASSWORD 'findsomethingmoresecure';



/*
ddl & dml
*/

drop table if exists  api_data.tgen_code_value;
create  UNLOGGED table api_data.tgen_code_value 
(
ID              SERIAL PRIMARY KEY,
TS              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
CODE_TYPE_ID       INTEGER,
CODE_TYPE_TEXT       TEXT,
CODE_ID         INTEGER,
CODE_VALUE      TEXT
);

INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(1, 'trace message type', 1, 'Step execution');


INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(2, 'trace message type', 1, 'SQL script fullname');

INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(1, 'trace message type', 2, 'SQL filters');

INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(1, 'trace message type', 3, 'SQL identifiers');

INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(1, 'trace message type', 4, 'SQL joins count');


INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(1, 'trace message type', 6, 'SQL Subselect count');

INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(2, 'trace message level', 1, 'Info');

INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(2, 'trace message level', 2, 'Error');

INSERT INTO api_data.tgen_code_value 
(CODE_TYPE_ID, CODE_TYPE_TEXT, CODE_ID, CODE_VALUE)
VALUES
(2, 'trace message level', 3, 'Debug');

drop table if exists  api_data.trun_traces;
create  UNLOGGED table api_data.trun_traces 
(
ID                  SERIAL PRIMARY KEY,
TS                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
UUID                TEXT,
MESSAGE_LEVEL       INTEGER,
MESSAGE_TYPE_ID     INTEGER,
MESSAGE_TEXT        TEXT
);

INSERT INTO api_data.tgen_code_value 
(MESSAGE_LEVEL, MESSAGE_TYPE_ID, MESSAGE_TEXT)
VALUES
(2, 1, 'dffff');

-- analysis jobs duration
select message_text ,
min(case when message_type_id = 1 then ts else null end),
max(case when message_type_id = 2 then ts else null end),
max(case when message_type_id = 2 then ts else null end) - min(case when message_type_id = 1 then ts else null end),
count(*) 
from api_data.trun_traces a
where a.message_level = 2
group by 
a.message_text 
order by 4 desc nulls first;


drop table if exists  api_data.tsql_analysis_traces;
create  UNLOGGED table api_data.tsql_analysis_traces 
(
ID                  SERIAL PRIMARY KEY,
TS                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
FILENAME            TEXT,
QUERY               TEXT,
UUID                TEXT,
ANALYZER_ID         INT,
ANALYZER_KEY        TEXT,
ANALYZER_VALUE      TEXT,
VALUE_TYPE          TEXT
);
