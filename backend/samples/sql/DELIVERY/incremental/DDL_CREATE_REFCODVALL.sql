DROP TABLE REFCODVALL IF EXISTS;

CREATE TABLE REFCODVALL
(
    REFCODVALL_ID     BIGINT       NOT NULL,
    REFCODVAL_ID      BIGINT,
    CREUSR_CF         CHARACTER(4),
    LSTUPDUSR_CF      CHARACTER(4),
    LANGUAGE_CODE     CHARACTER(5),
    REFERENCE_CODE_LS VARCHAR(32),
    REFERENCE_CODE_LM VARCHAR(64),
    REFERENCE_CODE_LL VARCHAR(128),
    CREATION_DT       DATE,
    LAST_UPDATE_DT    DATE,
    CONSTRAINT PK_REFCODVALL
    PRIMARY KEY (REFCODVALL_ID)
)
     ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;