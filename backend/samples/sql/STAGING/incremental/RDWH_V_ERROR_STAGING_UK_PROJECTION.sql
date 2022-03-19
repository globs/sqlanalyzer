SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_STAGING_UK_PROJECTION;

CREATE OR REPLACE VIEW V_ERROR_STAGING_UK_PROJECTION AS (
SELECT
    AA.LINE_NUMBER,
    AA.COL_NUMBER ,
    AA.COL_VALUE ,
    AA.ERROR_DESC ,
    E.ERROR_MESSAGE_ID
FROM
    (
    SELECT
        ST.LINE_NUMBER ,
        1 AS COL_NUMBER ,
        ST.JOINKEY AS COL_VALUE,
        CASE
            WHEN ST.JOINKEY = ''
            OR ST.JOINKEY IS NULL
            OR REGEXP_REPLACE(ST.JOINKEY,
            '[!@#$%^&*+=' || CHR(34)|| ':{}<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN ST.JOINKEY = ''
                OR ST.JOINKEY IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_REPLACE(ST.JOINKEY,
                '[!@#$%^&*+=' || CHR(34)|| ':{}<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
 STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        2 AS COL_NUMBER ,
        ST.PRODUCT AS COL_VALUE,
        CASE
            WHEN ST.PRODUCT = ''
            OR ST.PRODUCT IS NULL
            OR LENGTH(ST.PRODUCT)>12
            OR REGEXP_REPLACE(ST.PRODUCT,
            '[!@#$%^&*+=' || CHR(34)|| ':{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN ST.PRODUCT = ''
                OR ST.PRODUCT IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.PRODUCT)>12 THEN '006 - Data length is not valid'
                WHEN REGEXP_REPLACE(ST.PRODUCT,
                '[!@#$%^&*+=' || CHR(34)|| ':{}|<>\;,/?]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        3 AS COL_NUMBER ,
        ST.OMEGATREATYNUMBER AS COL_VALUE,
        CASE
            WHEN ST.OMEGATREATYNUMBER = ''
            OR ST.OMEGATREATYNUMBER IS NULL
            OR LENGTH(ST.OMEGATREATYNUMBER)<> 9
            OR REGEXP_REPLACE(ST.OMEGATREATYNUMBER,
            '[!@#$%^&*+=' || CHR(34)|| ':{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN ST.OMEGATREATYNUMBER = ''
                OR ST.OMEGATREATYNUMBER IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.OMEGATREATYNUMBER)<> 9 THEN '006 - Data length is not valid'
                WHEN REGEXP_REPLACE(ST.OMEGATREATYNUMBER,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        4 AS COL_NUMBER ,
        ST.OMEGASECTION AS COL_VALUE,
        CASE
            WHEN ST.OMEGASECTION = ''
            OR ST.OMEGASECTION IS NULL
            OR LENGTH(ST.OMEGASECTION)>2
            OR LENGTH(REGEXP_REPLACE(ST.OMEGASECTION,
            '[-0-9]',
            '')) > 0
            OR TRIM(ST.OMEGASECTION) LIKE ('-%') THEN
            CASE
                WHEN ST.OMEGASECTION = ''
                OR ST.OMEGASECTION IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.OMEGASECTION)>2 THEN '006 - Data length is not valid'
                WHEN LENGTH(REGEXP_REPLACE(ST.OMEGASECTION,
                '[-0-9]',
                '')) > 0
                OR TRIM(ST.OMEGASECTION) LIKE ('-%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        5 AS COL_NUMBER ,
        ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER AS COL_VALUE,
        CASE
            WHEN ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER = '' THEN 'SUCCESS'
            WHEN (ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER <> ''
            AND LENGTH(ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER)<> 9)
            OR REGEXP_REPLACE(ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER,
            '[!@#$%^&*+=' || CHR(34)|| ':{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN LENGTH(ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER)<> 9 THEN '006 - Data length is not valid'
                WHEN REGEXP_REPLACE(ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        6 AS COL_NUMBER ,
        ST.GROSS_ASSUMED_OMEGA_SECTION AS COL_VALUE,
        CASE
            WHEN LENGTH(ST.GROSS_ASSUMED_OMEGA_SECTION)>2
            OR LENGTH(REGEXP_REPLACE(ST.GROSS_ASSUMED_OMEGA_SECTION,
            '[-0-9]',
            '')) > 0
            OR TRIM(ST.GROSS_ASSUMED_OMEGA_SECTION) LIKE ('-%') THEN
            CASE
                WHEN LENGTH(ST.GROSS_ASSUMED_OMEGA_SECTION)>2 THEN '006 - Data length is not valid'
                WHEN LENGTH(REGEXP_REPLACE(ST.GROSS_ASSUMED_OMEGA_SECTION,
                '[-0-9]',
                '')) > 0
                OR TRIM(ST.GROSS_ASSUMED_OMEGA_SECTION) LIKE ('-%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        7 AS COL_NUMBER ,
        ST.RUNNUMBER AS COL_VALUE,
        CASE
            WHEN ST.RUNNUMBER = ''
            OR ST.RUNNUMBER IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(REGEXP_REPLACE(ST.RUNNUMBER,
            '[-0-9]',
            '')) > 0
            OR TRIM(ST.RUNNUMBER) LIKE ('-%') THEN '004 - Data type is not valid'
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        8 AS COL_NUMBER ,
        ST.AOCSTEP AS COL_VALUE,
        CASE
            WHEN ST.AOCSTEP = ''
            OR ST.AOCSTEP IS NULL
            OR LENGTH(ST.AOCSTEP)>100 THEN
            CASE
                WHEN ST.AOCSTEP = ''
                OR ST.AOCSTEP IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.AOCSTEP)>100 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        9 AS COL_NUMBER ,
        ST.SENSITIVITY_TYPE AS COL_VALUE,
        CASE
            WHEN ST.SENSITIVITY_TYPE = ''
            OR ST.SENSITIVITY_TYPE IS NULL
            OR LENGTH(ST.SENSITIVITY_TYPE)>100 THEN
            CASE
                WHEN ST.SENSITIVITY_TYPE = ''
                OR ST.SENSITIVITY_TYPE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.SENSITIVITY_TYPE)>100 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        10 AS COL_NUMBER ,
        ST.SENSITIVITY_VALUE AS COL_VALUE,
        CASE
            WHEN REGEXP_REPLACE(ST.SENSITIVITY_VALUE,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR ST.SENSITIVITY_VALUE IS NULL
            OR ST.SENSITIVITY_VALUE = ''
            OR TRIM(ST.SENSITIVITY_VALUE) LIKE ('-') THEN
            CASE
                WHEN ST.SENSITIVITY_VALUE = ''
                OR ST.SENSITIVITY_VALUE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.SENSITIVITY_VALUE) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        11 AS COL_NUMBER ,
        ST.SPCODE AS COL_VALUE,
        CASE
            WHEN ST.SPCODE IS NULL
            OR ST.SPCODE = ''
            OR TRIM(ST.SPCODE) LIKE ('-') THEN
            CASE
                WHEN ST.SPCODE = ''
                OR ST.SPCODE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(REGEXP_REPLACE(ST.SPCODE,
                '[-0-9]',
                '')) > 0
                OR TRIM(ST.SPCODE) LIKE ('-') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        12 AS COL_NUMBER ,
        ST.POLICY_UWY AS COL_VALUE,
        CASE
            WHEN LENGTH(ST.POLICY_UWY)>4
            OR LENGTH(REGEXP_REPLACE(ST.POLICY_UWY,
            '[-0-9]',
            '')) > 0
            OR TRIM(ST.POLICY_UWY) LIKE ('-%') THEN
            CASE
                WHEN LENGTH(ST.POLICY_UWY)>4 THEN '006 - Data length is not valid'
                WHEN LENGTH(REGEXP_REPLACE(ST.POLICY_UWY,
                '[-0-9]',
                '')) > 0
                OR TRIM(ST.POLICY_UWY) LIKE ('-%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        13 AS COL_NUMBER ,
        ST.BUSINESS_MATURITY AS COL_VALUE,
        CASE
            WHEN ST.BUSINESS_MATURITY = ''
            OR ST.BUSINESS_MATURITY IS NULL
            OR LENGTH(ST.BUSINESS_MATURITY)>50 THEN
            CASE
                WHEN ST.BUSINESS_MATURITY = ''
                OR ST.BUSINESS_MATURITY IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.BUSINESS_MATURITY)>50 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        14 AS COL_NUMBER ,
        ST.NEW_BUSINESS AS COL_VALUE,
        CASE
        WHEN ST.NEW_BUSINESS = ''
                OR ST.NEW_BUSINESS IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(ST.NEW_BUSINESS)>1
            OR LENGTH(REGEXP_REPLACE(ST.NEW_BUSINESS,
            '[-0-9]',
            '')) > 0
            OR TRIM(ST.NEW_BUSINESS) LIKE ('-') THEN
            CASE
            
                WHEN LENGTH(ST.NEW_BUSINESS)>1 THEN '006 - Data length is not valid'
                WHEN LENGTH(REGEXP_REPLACE(ST.NEW_BUSINESS,
                '[-0-9]',
                '')) > 0
                OR TRIM(ST.NEW_BUSINESS) LIKE ('-') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
   STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        15 AS COL_NUMBER ,
        ST.CLOSING_DATE AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.CLOSING_DATE,
            '[0-9]',
            '')) != 2
            OR REGEXP_REPLACE(ST.CLOSING_DATE,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.CLOSING_DATE)= 0
            OR ST.CLOSING_DATE IS NULL
            OR TRIM(ST.CLOSING_DATE) LIKE ('/') THEN
            CASE
                WHEN ST.CLOSING_DATE = ''
                OR ST.CLOSING_DATE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(REGEXP_REPLACE(ST.CLOSING_DATE,
                '[0-9]',
                '')) != 2
                OR REGEXP_REPLACE(ST.CLOSING_DATE,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,?]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        16 AS COL_NUMBER ,
        ST.PERIOD AS COL_VALUE,
        CASE
            WHEN ST.PERIOD IS NULL
            OR ST.PERIOD = ''
            OR LENGTH(REGEXP_REPLACE(ST.PERIOD,
            '[-0-9]',
            '')) > 0
            OR TRIM(ST.PERIOD) LIKE ('-') THEN
            CASE
                WHEN ST.PERIOD IS NULL
                OR ST.PERIOD = '' THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(REGEXP_REPLACE(ST.PERIOD,
                '[-0-9]',
                '')) > 0
                OR TRIM(ST.PERIOD) LIKE ('-%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        17 AS COL_NUMBER ,
        ST.TIME AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.TIME,
            '[0-9]',
            '')) != 2
            OR REGEXP_REPLACE(ST.TIME,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.TIME)= 0
            OR ST.TIME IS NULL
            OR TRIM(ST.TIME) LIKE ('/') THEN
            CASE
                WHEN ST.TIME = ''
                OR ST.TIME IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(REGEXP_REPLACE(ST.TIME,
                '[0-9]',
                '')) != 2
                OR REGEXP_REPLACE(ST.TIME,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,?]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        18 AS COL_NUMBER ,
        ST.REPORTING_BASIS AS COL_VALUE,
        CASE
            WHEN ST.REPORTING_BASIS = ''
            OR ST.REPORTING_BASIS IS NULL
            OR LENGTH(ST.REPORTING_BASIS)>100 THEN
            CASE
                WHEN ST.REPORTING_BASIS = ''
                OR ST.REPORTING_BASIS IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.REPORTING_BASIS)>100 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        19 AS COL_NUMBER ,
        ST.CURRENCY AS COL_VALUE,
        CASE
            WHEN ST.CURRENCY = ''
            OR ST.CURRENCY IS NULL
            OR LENGTH(ST.CURRENCY)<> 3
            OR REGEXP_REPLACE(ST.CURRENCY,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN ST.CURRENCY = ''
                OR ST.CURRENCY IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.CURRENCY)<> 3 THEN '006 - Data length is not valid'
                WHEN REGEXP_REPLACE(ST.CURRENCY,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        20 AS COL_NUMBER ,
        ST.EXPENSECURRENCY AS COL_VALUE,
        CASE
            WHEN ST.EXPENSECURRENCY = ''
            OR ST.EXPENSECURRENCY IS NULL
            OR LENGTH(ST.EXPENSECURRENCY)<> 3
            OR REGEXP_REPLACE(ST.EXPENSECURRENCY,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN ST.EXPENSECURRENCY = ''
                OR ST.EXPENSECURRENCY IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.EXPENSECURRENCY)<> 3 THEN '006 - Data length is not valid'
                WHEN REGEXP_REPLACE(ST.EXPENSECURRENCY,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        21 AS COL_NUMBER ,
        ST.PREM_INC AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.PREM_INC,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.PREM_INC,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.PREM_INC)= 0
            OR ST.PREM_INC IS NULL
            OR TRIM(ST.PREM_INC) LIKE ('-') THEN
            CASE
                WHEN ST.PREM_INC = ''
                OR ST.PREM_INC IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.PREM_INC) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        22 AS COL_NUMBER ,
        ST.OPT_PREM_INC AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.OPT_PREM_INC,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.OPT_PREM_INC,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.OPT_PREM_INC)= 0
            OR ST.OPT_PREM_INC IS NULL
            OR TRIM(ST.OPT_PREM_INC) LIKE ('-') THEN
            CASE
                WHEN ST.OPT_PREM_INC = ''
                OR ST.OPT_PREM_INC IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.OPT_PREM_INC) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        23 AS COL_NUMBER ,
        ST.EXT_PREM_INC AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.EXT_PREM_INC,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.EXT_PREM_INC,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.EXT_PREM_INC)= 0
            OR ST.EXT_PREM_INC IS NULL
            OR TRIM(ST.EXT_PREM_INC) LIKE ('-') THEN
            CASE
                WHEN ST.EXT_PREM_INC = ''
                OR ST.EXT_PREM_INC IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.EXT_PREM_INC) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        24 AS COL_NUMBER ,
        ST.CLAIMS_INCURRED AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.CLAIMS_INCURRED,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.CLAIMS_INCURRED,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.CLAIMS_INCURRED)= 0
            OR ST.CLAIMS_INCURRED IS NULL
            OR TRIM(ST.CLAIMS_INCURRED) LIKE ('-') THEN
            CASE
                WHEN ST.CLAIMS_INCURRED = ''
                OR ST.CLAIMS_INCURRED IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.CLAIMS_INCURRED) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        25 AS COL_NUMBER ,
        ST.CLAIMS_FROM_IBNP AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.CLAIMS_FROM_IBNP,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.CLAIMS_FROM_IBNP,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.CLAIMS_FROM_IBNP)= 0
            OR ST.CLAIMS_FROM_IBNP IS NULL
            OR TRIM(ST.CLAIMS_FROM_IBNP) LIKE ('-') THEN
            CASE
                WHEN ST.CLAIMS_FROM_IBNP = ''
                OR ST.CLAIMS_FROM_IBNP IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.CLAIMS_FROM_IBNP) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        26 AS COL_NUMBER ,
        ST.PROFIT_COMM AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.PROFIT_COMM,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.PROFIT_COMM,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.PROFIT_COMM)= 0
            OR ST.PROFIT_COMM IS NULL
            OR TRIM(ST.PROFIT_COMM) LIKE ('-') THEN
            CASE
                WHEN ST.PROFIT_COMM = ''
                OR ST.PROFIT_COMM IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.PROFIT_COMM) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        27 AS COL_NUMBER ,
        ST.IC_CLAWED_L AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.IC_CLAWED_L,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.IC_CLAWED_L,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.IC_CLAWED_L)= 0
            OR ST.IC_CLAWED_L IS NULL
            OR TRIM(ST.IC_CLAWED_L) LIKE ('-') THEN
            CASE
                WHEN ST.IC_CLAWED_L = ''
                OR ST.IC_CLAWED_L IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.IC_CLAWED_L) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        28 AS COL_NUMBER ,
        ST.REN_EXP_ATTRIBUTABLE AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.REN_EXP_ATTRIBUTABLE,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.REN_EXP_ATTRIBUTABLE,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.REN_EXP_ATTRIBUTABLE)= 0
            OR ST.REN_EXP_ATTRIBUTABLE IS NULL
            OR TRIM(ST.REN_EXP_ATTRIBUTABLE) LIKE ('-') THEN
            CASE
                WHEN ST.REN_EXP_ATTRIBUTABLE = ''
                OR ST.REN_EXP_ATTRIBUTABLE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.REN_EXP_ATTRIBUTABLE) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        29 AS COL_NUMBER ,
        ST.IBNR_RES_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.IBNR_RES_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.IBNR_RES_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.IBNR_RES_IF)= 0
            OR ST.IBNR_RES_IF IS NULL
            OR TRIM(ST.IBNR_RES_IF) LIKE ('-') THEN
            CASE
                WHEN ST.IBNR_RES_IF = ''
                OR ST.IBNR_RES_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.IBNR_RES_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        30 AS COL_NUMBER ,
        ST.OS_LOSS_RES AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.OS_LOSS_RES,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.OS_LOSS_RES,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.OS_LOSS_RES)= 0
            OR ST.OS_LOSS_RES IS NULL
            OR TRIM(ST.OS_LOSS_RES) LIKE ('-') THEN
            CASE
                WHEN ST.OS_LOSS_RES = ''
                OR ST.OS_LOSS_RES IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.OS_LOSS_RES) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
SELECT
        ST.LINE_NUMBER ,
        31 AS COL_NUMBER ,
        ST.SUM_ASSD_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.SUM_ASSD_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.SUM_ASSD_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.SUM_ASSD_IF)= 0
            OR ST.SUM_ASSD_IF IS NULL
            OR TRIM(ST.SUM_ASSD_IF) LIKE ('-') THEN
            CASE
                WHEN ST.SUM_ASSD_IF = ''
                OR ST.SUM_ASSD_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.SUM_ASSD_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
  STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        32 AS COL_NUMBER ,
        ST.INIT_COMM AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.INIT_COMM,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.INIT_COMM,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.INIT_COMM)= 0
            OR ST.INIT_COMM IS NULL
            OR TRIM(ST.INIT_COMM) LIKE ('-') THEN
            CASE
                WHEN ST.INIT_COMM = ''
                OR ST.INIT_COMM IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.INIT_COMM) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        33 AS COL_NUMBER ,
        ST.REN_EXP_NONATTRIBUTABLE AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.REN_EXP_NONATTRIBUTABLE,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.REN_EXP_NONATTRIBUTABLE,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.REN_EXP_NONATTRIBUTABLE)= 0
            OR ST.REN_EXP_NONATTRIBUTABLE IS NULL
            OR TRIM(ST.REN_EXP_NONATTRIBUTABLE) LIKE ('-') THEN
            CASE
                WHEN ST.REN_EXP_NONATTRIBUTABLE = ''
                OR ST.REN_EXP_NONATTRIBUTABLE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.REN_EXP_NONATTRIBUTABLE) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        34 AS COL_NUMBER ,
        ST.REN_COMM AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.REN_COMM,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.REN_COMM,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.REN_COMM)= 0
            OR ST.REN_COMM IS NULL
            OR TRIM(ST.REN_COMM) LIKE ('-') THEN
            CASE
                WHEN ST.REN_COMM = ''
                OR ST.REN_COMM IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.REN_COMM) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER,
        35 AS COL_NUMBER,
        ST.TAX AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.TAX,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.TAX,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.TAX)= 0
            OR ST.TAX IS NULL
            OR TRIM(ST.TAX) LIKE ('-') THEN
            CASE
                WHEN ST.TAX = ''
                OR ST.TAX IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.TAX) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER,
        36 AS COL_NUMBER,
        ST.NO_POLS_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.NO_POLS_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.NO_POLS_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.NO_POLS_IF)= 0
            OR ST.NO_POLS_IF IS NULL
            OR TRIM(ST.NO_POLS_IF) LIKE ('-') THEN
            CASE
                WHEN ST.NO_POLS_IF = ''
                OR ST.NO_POLS_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.NO_POLS_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        37 AS COL_NUMBER ,
        ST.UPR_RES_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.UPR_RES_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.UPR_RES_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.UPR_RES_IF)= 0
            OR ST.UPR_RES_IF IS NULL
            OR TRIM(ST.UPR_RES_IF) LIKE ('-') THEN
            CASE
                WHEN ST.UPR_RES_IF = ''
                OR ST.UPR_RES_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.UPR_RES_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        37 AS COL_NUMBER ,
        ST.MATH_RES_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.MATH_RES_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.MATH_RES_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.MATH_RES_IF)= 0
            OR ST.MATH_RES_IF IS NULL
            OR TRIM(ST.MATH_RES_IF) LIKE ('-') THEN
            CASE
                WHEN ST.MATH_RES_IF = ''
                OR ST.MATH_RES_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.MATH_RES_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        39 AS COL_NUMBER ,
        ST.EXT_PREM_RES AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.EXT_PREM_RES,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.EXT_PREM_RES,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.EXT_PREM_RES)= 0
            OR ST.EXT_PREM_RES IS NULL
            OR TRIM(ST.EXT_PREM_RES) LIKE ('-') THEN
            CASE
                WHEN ST.EXT_PREM_RES = ''
                OR ST.EXT_PREM_RES IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.EXT_PREM_RES) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        40 AS COL_NUMBER ,
        ST.CONT_RES_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.CONT_RES_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.CONT_RES_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.CONT_RES_IF)= 0
            OR ST.CONT_RES_IF IS NULL
            OR TRIM(ST.CONT_RES_IF) LIKE ('-') THEN
            CASE
                WHEN ST.CONT_RES_IF = ''
                OR ST.CONT_RES_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.CONT_RES_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        41 AS COL_NUMBER ,
        ST.PS_RESERVE AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.PS_RESERVE,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.PS_RESERVE,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.PS_RESERVE)= 0
            OR ST.PS_RESERVE IS NULL
            OR TRIM(ST.PS_RESERVE) LIKE ('-') THEN
            CASE
                WHEN ST.PS_RESERVE = ''
                OR ST.PS_RESERVE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.PS_RESERVE) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        42 AS COL_NUMBER ,
        ST.OPT_RES_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.OPT_RES_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.OPT_RES_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.OPT_RES_IF)= 0
            OR ST.OPT_RES_IF IS NULL
            OR TRIM(ST.OPT_RES_IF) LIKE ('-') THEN
            CASE
                WHEN ST.OPT_RES_IF = ''
                OR ST.OPT_RES_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.OPT_RES_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        43 AS COL_NUMBER ,
        ST.WOP_RES_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.WOP_RES_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.WOP_RES_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.WOP_RES_IF)= 0
            OR ST.WOP_RES_IF IS NULL
            OR TRIM(ST.WOP_RES_IF) LIKE ('-') THEN
            CASE
                WHEN ST.WOP_RES_IF = ''
                OR ST.WOP_RES_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.WOP_RES_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        44 AS COL_NUMBER ,
        ST.CIP_RES_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.CIP_RES_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.CIP_RES_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.CIP_RES_IF)= 0
            OR ST.CIP_RES_IF IS NULL
            OR TRIM(ST.CIP_RES_IF) LIKE ('-') THEN
            CASE
                WHEN ST.CIP_RES_IF = ''
                OR ST.CIP_RES_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.CIP_RES_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        45 AS COL_NUMBER ,
        ST.DAC_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.DAC_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.DAC_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.DAC_IF)= 0
            OR ST.DAC_IF IS NULL
            OR TRIM(ST.DAC_IF) LIKE ('-') THEN
            CASE
                WHEN ST.DAC_IF = ''
                OR ST.DAC_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.DAC_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        46 AS COL_NUMBER ,
        ST.EXTPRMDAC_IF AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.EXTPRMDAC_IF,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.EXTPRMDAC_IF,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.EXTPRMDAC_IF)= 0
            OR ST.EXTPRMDAC_IF IS NULL
            OR TRIM(ST.EXTPRMDAC_IF) LIKE ('-') THEN
            CASE
                WHEN ST.EXTPRMDAC_IF = ''
                OR ST.EXTPRMDAC_IF IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.EXTPRMDAC_IF) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        47 AS COL_NUMBER ,
        ST.RES_NONDEPINT AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.RES_NONDEPINT,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.RES_NONDEPINT,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.RES_NONDEPINT)= 0
            OR ST.RES_NONDEPINT IS NULL
            OR TRIM(ST.RES_NONDEPINT) LIKE ('-') THEN
            CASE
                WHEN ST.RES_NONDEPINT = ''
                OR ST.RES_NONDEPINT IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.RES_NONDEPINT) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        48 AS COL_NUMBER ,
        ST.DISC_PROF_B AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.DISC_PROF_B,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.DISC_PROF_B,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.DISC_PROF_B)= 0
            OR ST.DISC_PROF_B IS NULL
            OR TRIM(ST.DISC_PROF_B) LIKE ('-') THEN
            CASE
                WHEN ST.DISC_PROF_B = ''
                OR ST.DISC_PROF_B IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.DISC_PROF_B) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        49 AS COL_NUMBER ,
        ST.PROFIT AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.PROFIT,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.PROFIT,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.PROFIT)= 0
            OR ST.PROFIT IS NULL
            OR TRIM(ST.PROFIT) LIKE ('-') THEN
            CASE
                WHEN ST.PROFIT = ''
                OR ST.PROFIT IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.PROFIT) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        50 AS COL_NUMBER ,
        ST.ILS_PREM AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.ILS_PREM,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.ILS_PREM,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.ILS_PREM)= 0
            OR ST.ILS_PREM IS NULL
            OR TRIM(ST.ILS_PREM) LIKE ('-') THEN
            CASE
                WHEN ST.ILS_PREM = ''
                OR ST.ILS_PREM IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.ILS_PREM) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        51 AS COL_NUMBER ,
        ST.ANUITY_OUTGO AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.ANUITY_OUTGO,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.ANUITY_OUTGO,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.ANUITY_OUTGO)= 0
            OR ST.ANUITY_OUTGO IS NULL
            OR TRIM(ST.ANUITY_OUTGO) LIKE ('-') THEN
            CASE
                WHEN ST.ANUITY_OUTGO = ''
                OR ST.ANUITY_OUTGO IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.ANUITY_OUTGO) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        52 AS COL_NUMBER ,
        ST.ILS_CLAIMS AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.ILS_CLAIMS,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.ILS_CLAIMS,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.ILS_CLAIMS)= 0
            OR ST.ILS_CLAIMS IS NULL
            OR TRIM(ST.ILS_CLAIMS) LIKE ('-') THEN
            CASE
                WHEN ST.ILS_CLAIMS = ''
                OR ST.ILS_CLAIMS IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.ILS_CLAIMS) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        53 AS COL_NUMBER ,
        ST.ILS_PROFIT_COMM AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.ILS_PROFIT_COMM,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.ILS_PROFIT_COMM,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.ILS_PROFIT_COMM)= 0
            OR ST.ILS_PROFIT_COMM IS NULL
            OR TRIM(ST.ILS_PROFIT_COMM) LIKE ('-') THEN
            CASE
                WHEN ST.ILS_PROFIT_COMM = ''
                OR ST.ILS_PROFIT_COMM IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.ILS_PROFIT_COMM) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        54 AS COL_NUMBER ,
        ST.REN_EXP AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.REN_EXP,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.REN_EXP,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.REN_EXP)= 0
            OR ST.REN_EXP IS NULL
            OR TRIM(ST.REN_EXP) LIKE ('-') THEN
            CASE
                WHEN ST.REN_EXP = ''
                OR ST.REN_EXP IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.REN_EXP) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        55 AS COL_NUMBER ,
        ST.ILS_MAT_PROFIT_COMM AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.ILS_MAT_PROFIT_COMM,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.ILS_MAT_PROFIT_COMM,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.ILS_MAT_PROFIT_COMM)= 0
            OR ST.ILS_MAT_PROFIT_COMM IS NULL
            OR TRIM(ST.ILS_MAT_PROFIT_COMM) LIKE ('-') THEN
            CASE
                WHEN ST.ILS_MAT_PROFIT_COMM = ''
                OR ST.ILS_MAT_PROFIT_COMM IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.ILS_MAT_PROFIT_COMM) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        56 AS COL_NUMBER ,
        ST.COVERAGE_UNITS AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.COVERAGE_UNITS,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.COVERAGE_UNITS,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.COVERAGE_UNITS)= 0
            OR ST.COVERAGE_UNITS IS NULL
            OR TRIM(ST.COVERAGE_UNITS) LIKE ('-') THEN
            CASE
                WHEN ST.COVERAGE_UNITS = ''
                OR ST.COVERAGE_UNITS IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(ST.COVERAGE_UNITS) ,
                '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_UK_PROJECTIONS ST
    )AA
JOIN DELIVERY_<env>.ERROR_MESSAGE E ON
    E.ERROR_MESSAGE_LABEL = AA.ERROR_DESC );