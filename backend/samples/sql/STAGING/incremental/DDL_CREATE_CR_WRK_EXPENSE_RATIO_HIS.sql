SET SCHEMA STAGING_<env>;

DROP TABLE WRK_EXPENSE_RATIO_HIS IF EXISTS;

CREATE TABLE WRK_EXPENSE_RATIO_HIS
(
    CONTRACT_CATEGORY       INTEGER,
    RATIO_TYPE              CHARACTER(1),
    SEGMENT_LLOYDS          CHARACTER(1),
    SUBSIDIARY              SMALLINT,
    SUBLEDGER               SMALLINT,
    SEGMENT_CODE            VARCHAR(16),
    CONTRACT_NATURE         CHARACTER(1),
    CURRENCY                VARCHAR(3),
    CURRENCY_TYPE           VARCHAR(16),
    EXPENSES                DECFLOAT,
    DRIVER                  DECFLOAT,
    EXPENSE_RATIO           DECFLOAT,
    NORM_CODE               VARCHAR(16),
    SEGMENT_TYPE            VARCHAR(16),
    CREATED_DATE            TIMESTAMP(6)
)
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;