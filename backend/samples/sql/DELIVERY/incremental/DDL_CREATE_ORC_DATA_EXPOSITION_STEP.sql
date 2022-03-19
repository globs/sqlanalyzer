DROP TABLE DATA_EXPOSITION_STEP IF EXISTS;
CREATE TABLE DATA_EXPOSITION_STEP
(
	STEP_ID	                INT NOT NULL,
	SCHEMA_NAME				VARCHAR(128),
	TABLE_NAME				VARCHAR(128),
	DELIMITER				CHAR(1),
	INCLUDE_HEADER			BOOLEAN,
	SPECIFIC_PROCESS        VARCHAR(10000),
	FILE_DIRECTORY	        VARCHAR(256),
	FILE_NAME_PATTERN	    VARCHAR(512),
	DATA_FILE_TREATMENT		VARCHAR(32)
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE DATA_EXPOSITION_STEP ADD CONSTRAINT DATA_EXPOSITION_STEP_PK PRIMARY KEY (STEP_ID);