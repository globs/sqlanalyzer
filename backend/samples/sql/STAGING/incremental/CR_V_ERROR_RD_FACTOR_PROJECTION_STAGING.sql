SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_RD_FACTOR_PROJECTION_STAGING;

CREATE OR REPLACE VIEW V_ERROR_RD_FACTOR_PROJECTION_STAGING AS
    (
        SELECT
            LINE_NUMBER
          , CASE
                WHEN CONTRACT_NUMBER          = ''
                    OR CONTRACT_NUMBER  IS NULL
                    OR LENGTH(CONTRACT_NUMBER)>9
                    THEN 'error'
                    ELSE 'success'
            END AS CONTRACT_NUMBER_CHECK
          , CONTRACT_NUMBER
          , CASE
                WHEN SECTION_NUMBER                                         = ''
                    OR SECTION_NUMBER                                 IS NULL
                    OR LENGTH(regexp_replace(SECTION_NUMBER, '[-0-9]', '')) > 0
                    OR TRIM(SECTION_NUMBER)                              LIKE ('-')
                    THEN 'error'
                    ELSE 'success'
            END AS SECTION_NUMBER_CHECK
          , SECTION_NUMBER
          , CASE
                WHEN LENGTH(SECTION_NUMBER)                      >4
                    OR LENGTH(regexp_replace(UWY, '[-0-9]', '')) > 0
                    OR TRIM(UWY)                              LIKE ('-')
                    THEN 'error'
                    ELSE 'success'
            END AS UWY_CHECK
          , UWY
          , CASE
                WHEN AOC_STEP          = ''
                    OR AOC_STEP  IS NULL
                    OR LENGTH(AOC_STEP)>100
                    THEN 'error'
                    ELSE 'success'
            END AS AOC_STEP_check
          , AOC_STEP
          , CASE
                WHEN CSM_CASHFLOW          = ''
                    OR CSM_CASHFLOW  IS NULL
                    OR LENGTH(CSM_CASHFLOW)>100
                    THEN 'error'
                    ELSE 'success'
            END AS CSM_CASHFLOW_check
          , CSM_CASHFLOW
          , CASE
                WHEN PV_FLAG         = ''
                    OR PV_FLAG IS NULL
                    OR regexp_replace(PV_FLAG, '[!@#$%^&*+='
                        ||chr(34)
                        ||':{}|<>\;,./?-]', '~') LIKE ('%~%')
                    OR LENGTH(PV_FLAG)              >1
                    THEN 'error'
                    ELSE 'success'
            END AS PV_FLAG_CHECK
          , PV_FLAG
          , CASE
                WHEN PERIOD_ID                                         = ''
                    OR PERIOD_ID                                 IS NULL
                    OR LENGTH(PERIOD_ID)                               >1
                    OR LENGTH(regexp_replace(PERIOD_ID, '[-0-9]', '')) > 0
                    OR TRIM(PERIOD_ID)                              LIKE ('-')
                    THEN 'error'
                    ELSE 'success'
            END AS PERIOD_ID_CHECK
          , PERIOD_ID
          , CASE
                WHEN PERIOD                                         = ''
                    OR PERIOD                                 IS NULL
                    OR LENGTH(PERIOD)                               >2
                    OR LENGTH(regexp_replace(PERIOD, '[-0-9]', '')) > 0
                    OR TRIM(PERIOD)                              LIKE ('-')
                    THEN 'error'
                    ELSE 'success'
            END AS PERIOD_CHECK
          , PERIOD
          , CASE
                WHEN YEAR                                         = ''
                    OR YEAR                                 IS NULL
                    OR LENGTH(YEAR)                               >4
                    OR LENGTH(regexp_replace(YEAR, '[-0-9]', '')) > 0
                    OR TRIM(YEAR)                              LIKE ('-')
                    THEN 'error'
                    ELSE 'success'
            END AS YEAR_CHECK
          , YEAR
          , CASE
                WHEN LENGTH(regexp_replace(REPLACE(RA_FACTOR, chr(13), ''), '[-0-9]', '')) <= 1
                    AND regexp_replace(RA_FACTOR, '[!@#$%^&*+='
                        ||chr(34)
                        ||':_{}|<>\;,/?]', '~') NOT LIKE ('%~%')
                    AND TRIM(RA_FACTOR)         NOT LIKE ('-')
                    THEN 'success'
                    ELSE 'error'
            END                             AS RA_FACTOR_check
          , REPLACE(RA_FACTOR, chr(13), '') AS RA_FACTOR
        FROM
            STAGING_RD_FACTOR_PROJECTION
    )
;