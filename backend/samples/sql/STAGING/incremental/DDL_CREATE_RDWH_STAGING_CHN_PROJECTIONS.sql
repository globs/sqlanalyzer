SET SCHEMA STAGING_<env>;

DROP TABLE STAGING_CHN_PROJECTIONS IF EXISTS;

CREATE TABLE STAGING_CHN_PROJECTIONS
(
    JOINKEY                           VARCHAR(255),
    PRODUCT                           VARCHAR(255),
    OMEGA_TREATY_NUMBER               VARCHAR(255),
    OMEGA_SECTION                     VARCHAR(255),
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER VARCHAR(255),
    GROSS_ASSUMED_OMEGA_SECTION       VARCHAR(255),
    RUNNUMBER                         VARCHAR(255),
    AOCSTEP                           VARCHAR(255),
    SENSITIVITYTYPE                   VARCHAR(255),
    SENSITIVITYVALUE                  VARCHAR(255),
    SPCODE                            VARCHAR(255),
    POLICYUWY                         VARCHAR(255),
    BUSINESSMATURITY                  VARCHAR(255),
    NEWBUSINESS                       VARCHAR(255),
    CLOSINGDATE                       VARCHAR(255),
    P_PERIOD                          VARCHAR(255),
    P_TIME                            VARCHAR(255),
    REPORTINGBASIS                    VARCHAR(255),
    CURRENCY                          VARCHAR(255),
    PREM_WRITTEN                      VARCHAR(255),
    CLAIMS_INCURRED                   VARCHAR(255),
    CLAIMS_FROM_IBNP                  VARCHAR(255),
    COLLATERAL_COST                   VARCHAR(255),
    TOT_COMM                          VARCHAR(255),
    FIN_COMM                          VARCHAR(255),
    FIN_BROK_FEE                      VARCHAR(255),
    RETRO_OVERRIDE_COMM               VARCHAR(255),
    PS_TOTAL_ALLOCATION               VARCHAR(255),
    DEP_MATH_RES_IF                   VARCHAR(255),
    DEP_UNEARN_PREM_RES_IF            VARCHAR(255),
    DEP_INC_RISK_RES_IF               VARCHAR(255),
    NDEP_MATH_RES_IF                  VARCHAR(255),
    NDEP_UNEARN_PREM_RES_IF           VARCHAR(255),
    NDEP_INC_RISK_RES_IF              VARCHAR(255),
    DEP_IBNP_RES_IF                   VARCHAR(255),
    NDEP_IBNP_RES_IF                  VARCHAR(255),
    DAC_IF                            VARCHAR(255),
    RES_DEPINT                        VARCHAR(255),
    RES_NONDEPINT                     VARCHAR(255),
    TOT_REN_EXP                       VARCHAR(255),
    TOT_ACQ_EXP                       VARCHAR(255),
    TOT_INV_EXP                       VARCHAR(255),
    TAX                               VARCHAR(255),
    SUM_ASSD_IF                       VARCHAR(255),
    VOBA                              VARCHAR(255),
    PV_PS_TOTAL_ALLOCATION            VARCHAR(255),
    PREM_TAX                          VARCHAR(255),
    PREM_REFUND                       VARCHAR(255),
    INIT_COMM                         VARCHAR(255),
    COMM_CLAWBACK                     VARCHAR(255),
    REN_COMM                          VARCHAR(255),
    REN_EXP_ATTRIBUTABLE              VARCHAR(255),
    ACQ_EXP_ATTRIBUTABLE              VARCHAR(255),
    INV_EXP_ATTRIBUTABLE              VARCHAR(255),
    CLAIM_EXP_ATTRIBUTABLE            VARCHAR(255),
    REN_EXP_NONATTRIBUTABLE           VARCHAR(255),
    ACQ_EXP_NONATTRIBUTABLE           VARCHAR(255),
    INV_EXP_NONATTRIBUTABLE           VARCHAR(255),
    CLAIM_EXP_NONATTRIBUTABLE         VARCHAR(255),
    COVERAGE_UNITS                    VARCHAR(255),
    LINE_NUMBER                       VARCHAR(255)
)
    ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;

