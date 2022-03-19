DROP TABLE TBUSINESS_CALENDAR_PERIODS IF EXISTS;
CREATE TABLE TBUSINESS_CALENDAR_PERIODS
(
    CLOSING_PERIOD_ID   INTEGER      NOT NULL,
    CLOSING_PERIOD_LB   VARCHAR(255) NOT NULL,
    CLOSING_CAL_START_D TIMESTAMP(6) NOT NULL,
    CLOSING_CAL_END_D   TIMESTAMP(6) NOT NULL,
    CLOSING_QUARTER     INTEGER      NOT NULL,
    CLOSING_YEAR        INTEGER      NOT NULL,
    CLOSING_NORM        VARCHAR(255) NOT NULL,
    CLOSING_SUBNORM     VARCHAR(255) NOT NULL,
    CLOSING_TYPE        VARCHAR(255),
    RUN_TYPE            VARCHAR(16),
    CONSTRAINT TBUSINESS_CALENDAR_PERIODS_PK
    PRIMARY KEY (CLOSING_PERIOD_ID)
)  
ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;
		 
		 