DROP TABLE BUSINESS_EVENT_ALERT IF EXISTS;
CREATE TABLE BUSINESS_EVENT_ALERT
(
	BUSINESS_EVENT_ALERT_ID			     INT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
	BUSINESS_EVENT_ID					 INT,
	ENTITY								 VARCHAR(64),
	MAILING_LIST						 VARCHAR(2500),
	ALERT_TYPE							 VARCHAR(64),
	IS_ACTIVE							 BOOLEAN,
	DELAY_DAYS							 VARCHAR(32)
 ) ORGANIZE BY ROW DISTRIBUTE ON RANDOM IN TBS_<env>;
ALTER TABLE BUSINESS_EVENT_ALERT ADD CONSTRAINT BUSINESS_EVENT_ALERT_PK PRIMARY KEY (BUSINESS_EVENT_ALERT_ID);