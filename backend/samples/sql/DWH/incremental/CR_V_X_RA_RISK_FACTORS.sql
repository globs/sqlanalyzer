SET SQL_COMPAT='NPS';

DROP VIEW V_X_RA_RISK_FACTORS;

CREATE OR REPLACE VIEW V_X_RA_RISK_FACTORS AS
SELECT
    REPORTING_DT,
    SCENARIO_ID,
    ENTITY_ID,
    INSURANCE_CONTRACT_GROUP_ID,
    CASHFLOW_LEG_NM,
    CASHFLOW_TYPE_CD,
    CASHFLOW_DT,
    RISK_FACTOR_RT,
    NORM_CODE,
    DATA_TYPE
FROM
    (
    SELECT
        REPORTING_DT,
        SCENARIO_ID,
        ENTITY_ID,
        INSURANCE_CONTRACT_GROUP_ID,
        CASHFLOW_LEG_NM,
        CASHFLOW_TYPE_CD,
        CASHFLOW_DT,
        RISK_FACTOR_RT,
        NORM_CODE,
        DATA_TYPE,
        ROW_NUMBER() OVER (PARTITION BY REPORTING_DT,
        ENTITY_ID,
        INSURANCE_CONTRACT_GROUP_ID,
        CASHFLOW_LEG_NM,
        CASHFLOW_TYPE_CD,
        CASHFLOW_DT,
        NORM_CODE
    ORDER BY
        RISK_FACTOR_RT DESC) AS RN
    FROM
        (
        SELECT
            MAX(RAF.REPORTING_DT) AS REPORTING_DT,
            CAST (RAF.SCENARIO_ID AS SMALLINT) AS SCENARIO_ID,
            RAF.ENTITY_ID AS ENTITY_ID,
            RAF.INSURANCE_CONTRACT_GROUP_ID AS INSURANCE_CONTRACT_GROUP_ID,
            RAF.CASHFLOW_LEG_NM AS CASHFLOW_LEG_NM,
            CAST(RAF.CASHFLOW_TYPE_CD AS VARCHAR(10)) AS CASHFLOW_TYPE_CD,
            CAST(RAF.CASHFLOW_DT AS VARCHAR(10)) AS CASHFLOW_DT,
            RAF.RISK_FACTOR_RT AS RISK_FACTOR_RT,
            CAST(NORM_CODE AS SMALLINT) AS NORM_CODE,
            CAST(DATA_TYPE AS VARCHAR(9)) AS DATA_TYPE
        FROM
            (
            SELECT
                CAST(VARCHAR_FORMAT(A.CLOSING_DATE,
                'DD/MM/YYYY') AS VARCHAR(10)) AS REPORTING_DT,
                NULL AS SCENARIO_ID,
                CAST('SGL' AS VARCHAR(36)) AS ENTITY_ID,
                CAST(A.CONTRACT_NUMBER || '_' || A.SECTION_NUMBER || '_' || D.SECUWY_NF AS VARCHAR(36)) AS INSURANCE_CONTRACT_GROUP_ID,
                CAST(B.CODE AS VARCHAR(32)) AS CASHFLOW_LEG_NM,
                CAST(C.LOC_CODE_CSM AS VARCHAR(32)) AS CASHFLOW_TYPE_CD,
                CASE
                    WHEN A.PROJECTION_PERIOD = 1 THEN '31/01/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 2
                    AND DAYOFYEAR(CONCAT(A.PROJECTION_YEAR, '-12-31')) = 366 THEN '29/02/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 2
                    AND DAYOFYEAR(CONCAT(A.PROJECTION_YEAR, '-12-31')) = 365 THEN '28/02/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 3 THEN '31/03/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 4 THEN '30/04/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 5 THEN '31/05/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 6 THEN '30/06/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 7 THEN '31/07/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 8 THEN '31/08/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 9 THEN '30/09/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 10 THEN '31/10/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 11 THEN '30/11/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 12 THEN '31/12/' || A.PROJECTION_YEAR
                END AS CASHFLOW_DT,
                CAST(A.FACTOR AS DECIMAL(18,
                5)) AS RISK_FACTOR_RT,
                1 AS NORM_CODE,
                'CLOSING' AS DATA_TYPE
            FROM
                BI_<env>.RD_FACTOR_PROJECTION A
            INNER JOIN (
                SELECT
                    ID,
                    CODE,
                    VALID_TO
                FROM
                    BI_<env>.POSITION
                GROUP BY
                    ID,
                    CODE,
                    VALID_TO) B ON
                ( A.CALCULATED_POSITION_ID = B.ID
                AND B.VALID_TO = '9999-12-31'
                AND B.CODE IN ('LCL_PREM_AMT',
                'LCL_CLAIMS_AMT',
                'LCL_CLAIMS_PLT_AMT',
                'LCL_SUM_ASS_AMT',
                'LCL_TOT_NA_EXP_AMT'))
            INNER JOIN BI_<env>.LEVEL_OF_ANALYSIS C ON
                ( A.LEVEL_OF_ANALYSIS_ID = C.ID
                AND C.VALID_TO = '9999-12-31-00.00.00.000000')
            INNER JOIN DWHD1_<env>.V_TRTY_CHARACTERISTICS D ON
                (A.CONTRACT_NUMBER = D.CTR_NF
                AND A.SECTION_NUMBER = D.SEC_NF
                AND A.VALID_TO = '9999-12-31-00.00.00.000000')
            WHERE
                A.VALID_TO = '9999-12-31-00.00.00.000000'
                AND C.LOC_CODE_CSM IS NOT NULL
                AND LOC_CODE_CSM <> '') RAF
        GROUP BY
            SCENARIO_ID,
            ENTITY_ID,
            INSURANCE_CONTRACT_GROUP_ID,
            CASHFLOW_LEG_NM,
            CASHFLOW_TYPE_CD,
            CASHFLOW_DT,
            RISK_FACTOR_RT,
            NORM_CODE,
            DATA_TYPE)
UNION
    SELECT
        REPORTING_DT,
        SCENARIO_ID,
        ENTITY_ID,
        INSURANCE_CONTRACT_GROUP_ID,
        CASHFLOW_LEG_NM,
        CASHFLOW_TYPE_CD,
        CASHFLOW_DT,
        RISK_FACTOR_RT,
        NORM_CODE,
        DATA_TYPE,
        ROW_NUMBER() OVER (PARTITION BY REPORTING_DT,
        ENTITY_ID,
        INSURANCE_CONTRACT_GROUP_ID,
        CASHFLOW_LEG_NM,
        CASHFLOW_TYPE_CD,
        CASHFLOW_DT,
        NORM_CODE
    ORDER BY
        RISK_FACTOR_RT DESC) AS RN
    FROM
        (
        SELECT
            MAX(RAF.REPORTING_DT) AS REPORTING_DT,
            CAST (RAF.SCENARIO_ID AS SMALLINT) AS SCENARIO_ID,
            RAF.ENTITY_ID AS ENTITY_ID,
            RAF.INSURANCE_CONTRACT_GROUP_ID AS INSURANCE_CONTRACT_GROUP_ID,
            RAF.CASHFLOW_LEG_NM AS CASHFLOW_LEG_NM,
            CAST(RAF.CASHFLOW_TYPE_CD AS VARCHAR(10)) AS CASHFLOW_TYPE_CD,
            CAST(RAF.CASHFLOW_DT AS VARCHAR(10)) AS CASHFLOW_DT,
            RAF.RISK_FACTOR_RT AS RISK_FACTOR_RT,
            CAST(NORM_CODE AS SMALLINT) AS NORM_CODE,
            CAST(DATA_TYPE AS VARCHAR(9)) AS DATA_TYPE
        FROM
            (
            SELECT
                CAST(VARCHAR_FORMAT(A.CLOSING_DATE,
                'DD/MM/YYYY') AS VARCHAR(10)) AS REPORTING_DT,
                NULL AS SCENARIO_ID,
                CAST('SGL' AS VARCHAR(36)) AS ENTITY_ID,
                CAST(A.CONTRACT_NUMBER || '_' || A.SECTION_NUMBER || '_' || D.SECUWY_NF AS VARCHAR(36)) AS INSURANCE_CONTRACT_GROUP_ID,
                CAST(B.CODE AS VARCHAR(32)) AS CASHFLOW_LEG_NM,
                CAST(C.LOC_CODE_CSM AS VARCHAR(32)) AS CASHFLOW_TYPE_CD,
                CASE
                    WHEN A.PROJECTION_PERIOD = 1 THEN '31/01/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 2
                    AND DAYOFYEAR(CONCAT(A.PROJECTION_YEAR, '-12-31')) = 366 THEN '29/02/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 2
                    AND DAYOFYEAR(CONCAT(A.PROJECTION_YEAR, '-12-31')) = 365 THEN '28/02/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 3 THEN '31/03/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 4 THEN '30/04/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 5 THEN '31/05/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 6 THEN '30/06/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 7 THEN '31/07/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 8 THEN '31/08/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 9 THEN '30/09/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 10 THEN '31/10/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 11 THEN '30/11/' || A.PROJECTION_YEAR
                    WHEN A.PROJECTION_PERIOD = 12 THEN '31/12/' || A.PROJECTION_YEAR
                END AS CASHFLOW_DT,
                CAST(A.FACTOR AS DECIMAL(18,
                5)) AS RISK_FACTOR_RT,
                1 AS NORM_CODE,
                'GROUPING' AS DATA_TYPE
            FROM
                BI_<env>.RD_FACTOR_PROJECTION A
            INNER JOIN (
                SELECT
                    ID,
                    CODE,
                    VALID_TO
                FROM
                    BI_<env>.POSITION
                GROUP BY
                    ID,
                    CODE,
                    VALID_TO) B ON
                ( A.CALCULATED_POSITION_ID = B.ID
                AND B.VALID_TO = '9999-12-31'
                AND B.CODE IN ('LCL_PREM_AMT',
                'LCL_CLAIMS_AMT',
                'LCL_CLAIMS_PLT_AMT',
                'LCL_SUM_ASS_AMT',
                'LCL_TOT_NA_EXP_AMT'))
            INNER JOIN BI_<env>.LEVEL_OF_ANALYSIS C ON
                ( A.LEVEL_OF_ANALYSIS_ID = C.ID
                AND C.VALID_TO = '9999-12-31-00.00.00.000000')
            INNER JOIN DWHD1_<env>.V_TRTY_CHARACTERISTICS D ON
                (A.CONTRACT_NUMBER = D.CTR_NF
                AND A.SECTION_NUMBER = D.SEC_NF
                AND A.VALID_TO = '9999-12-31-00.00.00.000000')
            WHERE
                A.VALID_TO = '9999-12-31-00.00.00.000000'
                AND (D.PROFIT_SIGN_GRP IS NULL
                OR D.PROFIT_SIGN_GRP = '')
                AND C.LOC_CODE_CSM = 'EXR') RAF
        GROUP BY
            SCENARIO_ID,
            ENTITY_ID,
            INSURANCE_CONTRACT_GROUP_ID,
            CASHFLOW_LEG_NM,
            CASHFLOW_TYPE_CD,
            CASHFLOW_DT,
            RISK_FACTOR_RT,
            NORM_CODE,
            DATA_TYPE))
WHERE
    RN = 1;
