DROP TABLE CONF_CSM_RULES IF EXISTS;

CREATE TABLE CONF_CSM_RULES (
    RULE_CD             VARCHAR(1000) NOT NULL,
    RULE_TYPE           VARCHAR(1000) NOT NULL,
    IS_PRIOR_5Y         SMALLINT NOT NULL,
    PERIOD_TYPE_ID      SMALLINT NOT NULL,
    IS_CASH_FLOW        SMALLINT NOT NULL,
    PROJECTION_MONTH    SMALLINT NOT NULL,
    CASHFLOW_DT         VARCHAR(10) NOT NULL,
    COEFF_AMOUNT        DECFLOAT NOT NULL
) IN TBS_<env>;