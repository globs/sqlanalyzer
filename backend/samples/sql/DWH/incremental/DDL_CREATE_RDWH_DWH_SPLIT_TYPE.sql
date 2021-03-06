DROP TABLE DWH_SPLIT_TYPE IF EXISTS;

CREATE TABLE DWH_SPLIT_TYPE
(
    ID           SMALLINT     NOT NULL,
    CODE         VARCHAR(32)  NOT NULL,
    NAME         VARCHAR(64),
    CREATED_DATE TIMESTAMP(6) DEFAULT CURRENT TIMESTAMP,
    CONSTRAINT PK_DWH_SPLIT_TYPE
    PRIMARY KEY (ID)
)
    IN TBS_<env>
    DISTRIBUTE BY HASH (ID)
;