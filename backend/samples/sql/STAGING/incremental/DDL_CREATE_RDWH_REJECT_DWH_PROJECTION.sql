SET SCHEMA STAGING_<env>;

DROP TABLE REJECT_DWH_PROJECTION IF EXISTS;

CREATE TABLE REJECT_DWH_PROJECTION
(
    OMEGA_TREATY_NUMBER               VARCHAR(255),
    OMEGA_SECTION                     VARCHAR(255),
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER VARCHAR(255),
    GROSS_ASSUMED_OMEGA_SECTION       VARCHAR(255),
    SPLIT                             VARCHAR(255),
    AOC_STEP                          VARCHAR(255) NOT NULL,
    SENSITIVITY_TYPE                  VARCHAR(255) NOT NULL,
    SENSITIVITY_VALUE                 VARCHAR(255),
    POLICY_UWY                        VARCHAR(255),
    POSITION                          VARCHAR(255),
    CURRENCY                          VARCHAR(255),
    PRODUCT                           VARCHAR(255),
    BASIS                             VARCHAR(255) NOT NULL,
    CREATED_BY                        VARCHAR(20)  NOT NULL,
    CREATED_DATE                      TIMESTAMP(6) NOT NULL,
    REQUEST_ID                        BIGINT       NOT NULL,
    ERROR_MESSAGE_ID				  INTEGER	,
    CLOSING_DATE                      DATE         NOT NULL,
    BUSINESS_MATURITY                 VARCHAR(255),
    RETROOMEGATREATYNUMBER            VARCHAR(255),
    RETROOMEGASECTIONNUMBER           VARCHAR(255),
    LE                                VARCHAR(255),
    A_R                               VARCHAR(255),
    LE_HOP                            VARCHAR(255)
)
   ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;
