SET SCHEMA STAGING_<env>;

DROP TABLE STAGING_LRM_PROJECTION IF EXISTS;

CREATE TABLE STAGING_LRM_PROJECTION
(
    TREATY_NUMBER                 VARCHAR(100),
    SECTION_NUMBER                VARCHAR(100),
    POLICY_UNDERWRITING_YEAR      VARCHAR(100),
    REPORTING_BASIS_CODE          VARCHAR(100),
    BUSINESS_MATURITY_CODE        VARCHAR(100),
    CLOSING_DATE                  VARCHAR(100),
    GROSS_ASSUMED_CONTRACT_NUMBER VARCHAR(100),
    GROSS_ASSUMED_SECTION_NUMBER  VARCHAR(100),
    CURRENCY_CODE                 VARCHAR(100),
    CSM_CASHFLOW_LEGS_CODE        VARCHAR(100),
    SCENARIO_TYPE_CODE            VARCHAR(100),
    SCENARIO_PARAMETER            VARCHAR(100),
    LEVEL_OF_ANALYSIS_CODE        VARCHAR(100),
    PERIOD_ID                     VARCHAR(100),
    PERIOD                        VARCHAR(100),
    YEAR                          VARCHAR(100),
    VALUE                         VARCHAR(100),
    LINE_NUMBER                   VARCHAR(100)
)
 ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;