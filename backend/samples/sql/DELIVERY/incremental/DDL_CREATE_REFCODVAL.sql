DROP TABLE REFCODVAL IF EXISTS;

CREATE TABLE REFCODVAL
(
    REFCODVAL_ID         BIGINT        NOT NULL,
    REFCOD_ID            BIGINT,
    CREUSR_CF            CHARACTER(4),
    LSTUPDUSR_CF         CHARACTER(4),
    REFERENCE_CODE_VALUE CHARACTER(64),
    ACTIVE_CODE_VALUE_B  CHARACTER(1),
    CREATION_DT          DATE,
    LAST_UPDATE_DT       DATE,
    CONSTRAINT PK_REFCODVAL
    PRIMARY KEY (REFCODVAL_ID)
)
     ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;