SET SCHEMA STAGING_<env>;

DROP TABLE STAGING_RISK_ADJUSTMENT_FACTOR_PARAM IF EXISTS;

CREATE TABLE STAGING_RISK_ADJUSTMENT_FACTOR_PARAM
(            
    LINE_NUMBER                     VARCHAR(300),
    SUBSIDIARY_CODE                 VARCHAR(255),
    LEDGER_CODE                     VARCHAR(255),
    REGION_CODE                     VARCHAR(255),
    TREATY_LIFE_CHARACTERISTIC_CODE VARCHAR(255),
    FINANCING_TYPE_CODE             VARCHAR(255),
    IAS39_CODE                      VARCHAR(255),
    USGAAP_CODE                     VARCHAR(255),
    COINSURANCE_CODE                VARCHAR(255),
    TYPE_OF_BUSINESS_CODE           VARCHAR(255),
    OMEGA_LOB_CODE                  VARCHAR(255),
    GUARANTEE_CODE                  VARCHAR(255),
    POLICY_TYPE_CODE                VARCHAR(255),
    NATURE_CODE                     VARCHAR(255),
    BASIS_TYPE                      VARCHAR(255),
    COUNTER_PARTY_ID                VARCHAR(255),
    ACTUARIALMODELTYPE_CODE         VARCHAR(255),
    CLIENT_ID                       VARCHAR(255),
    TERMTYPE_CODE                   VARCHAR(255),
    BUSINESSMATURITY_CODE           VARCHAR(255),
    BUSINESSMATURITY_FLAG           VARCHAR(255),
    MARKET_UNIT_CODE                VARCHAR(255),
    LEVELOFANALYSIS_CODE            VARCHAR(255),
    CSM_CASHFLOW_LEGS_CODE          VARCHAR(255),
    PV_FLAG                         VARCHAR(255),
    PERIOD_ID                       VARCHAR(255),
    RA_PERIOD                       VARCHAR(255),
    RA_YEAR                         VARCHAR(255),
    RA_FACTOR                       VARCHAR(255),
    REPORTING_BASIS_CODE            VARCHAR(255),
    CLOSING_DATE                    VARCHAR(255),
    IFRS17_PORTFOLIO                VARCHAR(255),
    IFRS17_SUB_PORTFOLIO            VARCHAR(255),
    TRANSITION_MODE                 VARCHAR(255),
    INITIAL_PROFITABILITY           VARCHAR(255)
)
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;