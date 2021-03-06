DROP TABLE BUSINESS_VALUATION_INPUT IF EXISTS;
CREATE TABLE BUSINESS_VALUATION_INPUT
(
	BUSINESS_VALUATION_INPUT_ID    INT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
	BUSINESS_CALENDAR_ID                 INT,
	BUSINESS_CALENDAR_EXECUTION_ID       INT,
	BUSINESS_EVENT_PARAM_VALUATION_ID    INT,
	DATA_SET							 VARCHAR(32),
	PARENT_CLOSING_TYPE_CODE			 VARCHAR(32),
	PARENT_SCENARIO_TYPE			 	 VARCHAR(32),
	PARENT_RUN_TYPE			 			 VARCHAR(32),
	PARENT_CLOSING_PERIOD	             VARCHAR(32),
	DEFAULTING_RULE	                     VARCHAR(1024)
) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE BUSINESS_VALUATION_INPUT ADD CONSTRAINT BUSINESS_VALUATION_INPUT_PK PRIMARY KEY (BUSINESS_VALUATION_INPUT_ID);
