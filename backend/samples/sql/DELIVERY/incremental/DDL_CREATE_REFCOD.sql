DROP TABLE REFCOD IF EXISTS;

CREATE TABLE REFCOD
(
    REFCOD_ID      BIGINT        NOT NULL,
    CREUSR_CF      CHARACTER(4),
    LSTUPDUSR_CF   CHARACTER(4),
    REFERENCE_CODE CHARACTER(64),
    CREATION_DT    DATE,
    LAST_UPDATE_DT DATE,
    CONSTRAINT PK_REFCOD
    PRIMARY KEY (REFCOD_ID)
)
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;