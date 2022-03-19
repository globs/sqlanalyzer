DROP TABLE DWH_PERIOD_TYPE IF EXISTS;

CREATE TABLE DWH_PERIOD_TYPE
(
    ID           SMALLINT     NOT NULL,
    CODE         VARCHAR(32)  NOT NULL,
    NAME         VARCHAR(64),
    CREATED_DATE TIMESTAMP(6) DEFAULT CURRENT TIMESTAMP,
    CONSTRAINT PK_DWH_PERIOD_TYPE
    PRIMARY KEY (ID)
)
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;