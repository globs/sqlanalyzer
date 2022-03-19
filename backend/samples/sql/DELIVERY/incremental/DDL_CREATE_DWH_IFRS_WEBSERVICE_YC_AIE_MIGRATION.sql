DROP TABLE DWH_IFRS_WEBSERVICE_YC_AIE_MIGRATION IF EXISTS;

CREATE TABLE DWH_IFRS_WEBSERVICE_YC_AIE_MIGRATION
(   MIGRATION_DATE DATE NOT NULL,
    MIGRATION_TYPE VARCHAR(10) NOT NULL
)
ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;