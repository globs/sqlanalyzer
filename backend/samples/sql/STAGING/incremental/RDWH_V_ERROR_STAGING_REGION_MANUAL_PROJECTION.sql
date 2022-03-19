SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_STAGING_REGION_MANUAL_PROJECTION;

 CREATE OR REPLACE VIEW V_ERROR_STAGING_REGION_MANUAL_PROJECTION AS (
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
        ST.OMEGA_TREATY_NUMBER AS COL_VALUE,
        CASE
            WHEN ST.OMEGA_TREATY_NUMBER = ''
            OR ST.OMEGA_TREATY_NUMBER IS NULL
            OR LENGTH(ST.OMEGA_TREATY_NUMBER)<> 9
            OR REGEXP_REPLACE(ST.OMEGA_TREATY_NUMBER,
            '[!@#$%^&*+=' || CHR(34)|| ':{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN ST.OMEGA_TREATY_NUMBER = ''
                OR ST.OMEGA_TREATY_NUMBER IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.OMEGA_TREATY_NUMBER)<> 9 THEN '006 - Data length is not valid'
                WHEN REGEXP_REPLACE(ST.OMEGA_TREATY_NUMBER,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        2 AS COL_NUMBER ,
        ST.OMEGA_SECTION AS COL_VALUE,
        CASE
            WHEN ST.OMEGA_SECTION = ''
            OR ST.OMEGA_SECTION IS NULL
            OR LENGTH(ST.OMEGA_SECTION)>2
            OR LENGTH(REGEXP_REPLACE(ST.OMEGA_SECTION,
            '[-0-9]',
            '')) > 0
            OR TRIM(ST.OMEGA_SECTION) LIKE ('-%') THEN
            CASE
                WHEN ST.OMEGA_SECTION = ''
                OR ST.OMEGA_SECTION IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.OMEGA_SECTION)>2 THEN '006 - Data length is not valid'
                WHEN LENGTH(REGEXP_REPLACE(ST.OMEGA_SECTION,
                '[-0-9]',
                '')) > 0
                OR TRIM(ST.OMEGA_SECTION) LIKE ('-%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        3 AS COL_NUMBER ,
        ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER AS COL_VALUE,
        CASE
            WHEN LENGTH(ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER)> 9
            OR REGEXP_REPLACE(ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER,
            '[!@#$%^&*+=' || CHR(34)|| ':{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN LENGTH(ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER)> 9 THEN '006 - Data length is not valid'
                WHEN REGEXP_REPLACE(ST.GROSS_ASSUMED_OMEGA_TREATY_NUMBER,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        4 AS COL_NUMBER ,
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
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        5 AS COL_NUMBER ,
        ST.SPLIT AS COL_VALUE,
        CASE          
        WHEN LENGTH(ST.SPLIT)>100 THEN '006 - Data length is not valid'
            ELSE 'SUCCESS'
         END AS ERROR_DESC
    FROM
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        6 AS COL_NUMBER ,
        ST.AOC_STEP AS COL_VALUE,
        CASE
            WHEN ST.AOC_STEP = ''
            OR ST.AOC_STEP IS NULL
            OR LENGTH(ST.AOC_STEP)>100 THEN
            CASE
                WHEN ST.AOC_STEP = ''
                OR ST.AOC_STEP IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.AOC_STEP)>100 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        7 AS COL_NUMBER ,
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
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        8 AS COL_NUMBER ,
        ST.SENSITIVITY_VALUE AS COL_VALUE,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(ST.SENSITIVITY_VALUE,
            '[-0-9]',
            '')) > 1
            OR REGEXP_REPLACE(ST.SENSITIVITY_VALUE,
            '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(ST.SENSITIVITY_VALUE)= 0
            OR ST.SENSITIVITY_VALUE IS NULL
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
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        9 AS COL_NUMBER ,
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
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        10 AS COL_NUMBER ,
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
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        11 AS COL_NUMBER ,
        ST.POSITION AS COL_VALUE,
        CASE
            WHEN ST.POSITION = ''
            OR ST.POSITION IS NULL
            OR LENGTH(ST.POSITION)>100 THEN
            CASE
                WHEN ST.POSITION = ''
                OR ST.POSITION IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.POSITION)>100 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        12 AS COL_NUMBER ,
        ST.CURRENCY AS COL_VALUE,
        CASE
            WHEN ST.CURRENCY = ''
            OR ST.CURRENCY IS NULL
            OR LENGTH(ST.CURRENCY)<> 3
            OR REGEXP_REPLACE(ST.CURRENCY,
            '[!@#$%^&*+=' || CHR(34)|| ':{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN ST.CURRENCY = ''
                OR ST.CURRENCY IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.CURRENCY)<> 3 THEN '006 - Data length is not valid'
                WHEN REGEXP_REPLACE(ST.CURRENCY,
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        ST.LINE_NUMBER ,
        13 AS COL_NUMBER ,
        ST.BASIS AS COL_VALUE,
        CASE
            WHEN ST.BASIS = ''
            OR ST.BASIS IS NULL
            OR LENGTH(ST.BASIS)>100 THEN
            CASE
                WHEN ST.BASIS = ''
                OR ST.BASIS IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(ST.BASIS)>100 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'SUCCESS'
        END AS ERROR_DESC
    FROM
        STAGING_REGION_MANUAL_PROJECTION ST
UNION
    SELECT
        LINE_NUMBER,
        COL_NUMBER,
        COL_VALUE,
        ERROR_DESC
    FROM
        (
        SELECT
            ST.LINE_NUMBER AS LINE_NUMBER ,
            SYS.COLNO + 1 AS COL_NUMBER ,
            ST.AMOUNT AS COL_VALUE,
            PERIOD,
            CASE
                WHEN LENGTH(REGEXP_REPLACE(REPLACE(ST.AMOUNT, CHR(13), ''),
                '[-0-9]',
                '')) > 1
                OR REGEXP_REPLACE(REPLACE(ST.AMOUNT, CHR(13), ''),
                '[!@#$%^&*+=' || CHR(34)|| ':_{}|<>\;,/?]',
                '~') LIKE ('%~%')
                OR TRIM(ST.AMOUNT) LIKE ('-') THEN
                CASE
                    WHEN REGEXP_LIKE(TRIM(REPLACE(ST.AMOUNT, CHR(13), '')) ,
                    '^\D+(\.\D*)?$') != 'TRUE' THEN '004 - Data type is not valid'
                    ELSE '-1'
                END
                ELSE 'SUCCESS'
            END AS ERROR_DESC
        FROM
            STAGING_REGION_MANUAL_PROJECTION ST
        JOIN SYSCAT.COLUMNS SYS ON
            SYS.COLNAME = 'TEMP_' || PERIOD
            AND TABNAME = 'WRK_MANUAL_UPLOAD_DYNAMIC'
            AND TABSCHEMA = 'STAGING_UAT') )AA
JOIN DELIVERY_<env>."ERROR_MESSAGE" E ON
    E.ERROR_MESSAGE_LABEL = AA.ERROR_DESC );