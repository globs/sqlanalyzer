DROP TABLE BATCH_EXECUTION_PARAM IF EXISTS;
CREATE TABLE BATCH_EXECUTION_PARAM
(
	BATCH_EXECUTION_ID	            INT NOT NULL,
	PARAM_NAME	                    VARCHAR(256) NOT NULL,
	PARAM_VALUE	                    VARCHAR(5000),
	PARAM_WAY	                    VARCHAR(16),
	DATA_VALIDATION_CONTROL_ID		INT
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;