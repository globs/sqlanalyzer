SET SCHEMA STAGING_<env>;

DROP TABLE STAGING_CAN_AXIS_PROJECTIONS IF EXISTS;

CREATE TABLE STAGING_CAN_AXIS_PROJECTIONS  (
    NAME                                    VARCHAR(255) , 
    CURRENT_DATE_TIME                       VARCHAR(255) , 
    DATASET_LONG_NAME                       VARCHAR(255) , 
    REPORT_NAME                             VARCHAR(255) , 
    VALUATION_DATE                          VARCHAR(255) , 
    CLOSING_DATE                            VARCHAR(255) , 
    POLICY_UWY                              VARCHAR(255) , 
    BASIS                                   VARCHAR(255) , 
    BUSINESS_MATURITY                       VARCHAR(255) , 
    SENSITIVITY_TYPE                        VARCHAR(255) , 
    SENSITIVITY_VALUE                       VARCHAR(255) , 
    AOC_STEP                                VARCHAR(255) , 
    AOC_STEP_DETAIL                         VARCHAR(255) , 
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER       VARCHAR(255) , 
    GROSS_ASSUMED_OMEGA_TREATY_SECTION      VARCHAR(255) , 
    OMEGA_TREATY_NUMBER                     VARCHAR(255) , 
    OMEGA_SECTION                           VARCHAR(255) , 
    PRODUCT_TYPE                            VARCHAR(255) , 
    TYPE                                    VARCHAR(255) , 
    POSITION                                VARCHAR(255) , 
    CURRENCY                                VARCHAR(255) , 
    LINE_NUMBER                             VARCHAR(255) , 
    PERIOD                                  VARCHAR(255),
    PERIOD_DESCRIPTION                      VARCHAR(255),
    PROJECTION_AMOUNT                       VARCHAR(255)   
)
ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;