SET SCHEMA STAGING_<env>;

DROP TABLE WRK_DIP_PAPM_PREMIUMS IF EXISTS;

CREATE TABLE WRK_DIP_PAPM_PREMIUMS
(    
    YEAR            CHARACTER(4)  NOT NULL,
    SUBSIDIARY      INTEGER       NOT NULL,
    SUBLEDGER       INTEGER       NOT NULL,
    MARKET          VARCHAR(4)    NOT NULL,
    SEGMENT         CHARACTER(1)  NOT NULL,
    NATURE          VARCHAR(1)    NOT NULL,
    CURRENCY        VARCHAR(3)    NOT NULL,
    PREMIUMSAMOUNT  DECIMAL(31,2)
)
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;