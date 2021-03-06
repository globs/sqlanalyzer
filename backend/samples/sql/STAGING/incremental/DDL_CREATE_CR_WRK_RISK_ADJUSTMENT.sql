SET SCHEMA STAGING_<env>;

DROP TABLE WRK_RISK_ADJUSTMENT IF EXISTS;

CREATE TABLE WRK_RISK_ADJUSTMENT
(
    SUBSIDIARY              SMALLINT,
    SUBLEDGER               SMALLINT,
    SEGMENT_TYPE            VARCHAR(16),
    SEGMENT_CODE            VARCHAR(16),
    NORM_CODE               VARCHAR(16),
    CONTRACT_NATURE         CHARACTER(1),
    DOMAIN_CODE             VARCHAR(16),
    RESERVE_RA_FACTOR       DECFLOAT,
    PREMIUM_RA_FACTOR       DECFLOAT
)
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;