SET SCHEMA STAGING_<env>;

DROP TABLE STAGING_REGION_MANUAL_PROJECTION IF EXISTS;

CREATE TABLE STAGING_REGION_MANUAL_PROJECTION
(
    OMEGA_TREATY_NUMBER               VARCHAR(255),
    OMEGA_SECTION                     VARCHAR(255),
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER VARCHAR(255),
    GROSS_ASSUMED_OMEGA_SECTION       VARCHAR(255),
    SPLIT                             VARCHAR(255),
    AOC_STEP                          VARCHAR(255),
    SENSITIVITY_TYPE                  VARCHAR(255),
    SENSITIVITY_VALUE                 VARCHAR(255),
    POLICY_UWY                        VARCHAR(255),
    BUSINESS_MATURITY                 VARCHAR(255),
    POSITION                          VARCHAR(255),
    CURRENCY                          VARCHAR(255),
    BASIS                             VARCHAR(255),
    PERIOD                            VARCHAR(255),
    PERIOD_DESCRIPTION                VARCHAR(255),
    AMOUNT                            VARCHAR(255),
    HISTORIZATION_MODE                VARCHAR(255),
    CLOSING_DATE                      VARCHAR(255),
    LINE_NUMBER                       VARCHAR(255),
    REQUEST_ID                        BIGINT
)
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;