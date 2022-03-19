DROP TABLE DELIVERY_SWITCH_BI IF EXISTS;

CREATE TABLE DELIVERY_SWITCH_BI
(
    APPLI_N   VARCHAR(40)   NOT NULL,
    SCHEMA_N  VARCHAR(100)  NOT NULL,
    TABLE_N   VARCHAR(1020) NOT NULL,
    SYNONYM_N VARCHAR(1020),
    CONSTRAINT UC_DELIVERY_SWITCH_BI
    PRIMARY KEY (APPLI_N,SCHEMA_N,TABLE_N)
) 
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;