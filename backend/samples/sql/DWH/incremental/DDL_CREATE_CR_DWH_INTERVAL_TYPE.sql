DROP TABLE DWH_INTERVAL_TYPE IF EXISTS;

CREATE TABLE DWH_INTERVAL_TYPE
(
    ID           SMALLINT     NOT NULL,
    CODE         VARCHAR(32)  NOT NULL,
    NAME         VARCHAR(64)  NOT NULL,
    CREATED_DATE TIMESTAMP(6) DEFAULT CURRENT TIMESTAMP,
    CONSTRAINT PK_INTERVAL_TYPE
    PRIMARY KEY (ID)
)
   ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;