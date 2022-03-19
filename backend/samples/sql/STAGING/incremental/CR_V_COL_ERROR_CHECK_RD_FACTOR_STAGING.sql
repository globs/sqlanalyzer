SET SCHEMA STAGING_<env>;

DROP VIEW V_COL_ERROR_CHECK_RD_FACTOR_STAGING;

CREATE OR REPLACE VIEW V_COL_ERROR_CHECK_RD_FACTOR_STAGING AS
    (
        SELECT
            aa.LINE_NUMBER
          , aa.col_number
          , aa.col_value
          , aa.error_desc
          , e.error_message_id
        FROM
            (
                SELECT
                    st.LINE_NUMBER
                  , 2                  AS COL_NUMBER
                  , st.CONTRACT_NUMBER AS col_value
                  , CASE
                        WHEN st.CONTRACT_NUMBER          = ''
                            OR st.CONTRACT_NUMBER  IS NULL
                            OR LENGTH(st.CONTRACT_NUMBER)>9
                            THEN
                            CASE
                                WHEN st.CONTRACT_NUMBER         = ''
                                    OR st.CONTRACT_NUMBER IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.CONTRACT_NUMBER)>32
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 3                 AS COL_NUMBER
                  , st.SECTION_NUMBER AS col_value
                  , CASE
                        WHEN st.SECTION_NUMBER                                         = ''
                            OR st.SECTION_NUMBER                                 IS NULL
                            OR LENGTH(regexp_replace(st.SECTION_NUMBER, '[-0-9]', '')) > 0
                            OR TRIM(st.SECTION_NUMBER)                              LIKE ('-')
                            THEN
                            CASE
                                WHEN st.SECTION_NUMBER         = ''
                                    OR st.SECTION_NUMBER IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(regexp_replace(st.SECTION_NUMBER, '[-0-9]', '')) > 0
                                    OR TRIM(st.SECTION_NUMBER)                            LIKE ('-')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 4      AS COL_NUMBER
                  , st.UWY AS col_value
                  , CASE
                        WHEN st.UWY                                         = ''
                            OR st.UWY                                 IS NULL
                            OR LENGTH(regexp_replace(st.UWY, '[-0-9]', '')) > 0
                            OR TRIM(st.UWY)                              LIKE ('-')
                            THEN
                            CASE
                                WHEN LENGTH(regexp_replace(st.UWY, '[-0-9]', '')) > 0
                                    OR TRIM(st.UWY)                            LIKE ('-')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 5           AS COL_NUMBER
                  , st.AOC_STEP AS col_value
                  , CASE
                        WHEN st.AOC_STEP          = ''
                            OR st.AOC_STEP  IS NULL
                            OR LENGTH(st.AOC_STEP)>100
                            THEN
                            CASE
                                WHEN st.AOC_STEP         = ''
                                    OR st.AOC_STEP IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.AOC_STEP)>100
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 6               AS COL_NUMBER
                  , st.CSM_CASHFLOW AS col_value
                  , CASE
                        WHEN st.CSM_CASHFLOW          = ''
                            OR st.CSM_CASHFLOW  IS NULL
                            OR LENGTH(st.CSM_CASHFLOW)>100
                            THEN
                            CASE
                                WHEN st.CSM_CASHFLOW         = ''
                                    OR st.CSM_CASHFLOW IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.CSM_CASHFLOW)>100
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 7          AS COL_NUMBER
                  , st.PV_FLAG AS col_value
                  , CASE
                        WHEN st.PV_FLAG          = ''
                            OR st.PV_FLAG  IS NULL
                            OR LENGTH(st.PV_FLAG)>1
                            THEN
                            CASE
                                WHEN st.PV_FLAG         = ''
                                    OR st.PV_FLAG IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.PV_FLAG)>1
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 8            AS COL_NUMBER
                  , st.PERIOD_ID AS col_value
                  , CASE
                        WHEN st.PERIOD_ID                                         = ''
                            OR st.PERIOD_ID                                 IS NULL
                            OR LENGTH(st.PERIOD_ID)                               >1
                            OR LENGTH(regexp_replace(st.PERIOD_ID, '[-0-9]', '')) > 0
                            OR TRIM(st.PERIOD_ID)                              LIKE ('-')
                            THEN
                            CASE
                                WHEN st.PERIOD_ID         = ''
                                    OR st.PERIOD_ID IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.PERIOD_ID)>1
                                    THEN '006 - Data length is not valid'
                                WHEN LENGTH(regexp_replace(st.PERIOD_ID, '[-0-9]', '')) > 0
                                    OR TRIM(st.PERIOD_ID)                            LIKE ('-')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 9         AS COL_NUMBER
                  , st.PERIOD AS col_value
                  , CASE
                        WHEN st.PERIOD                                         = ''
                            OR st.PERIOD                                 IS NULL
                            OR LENGTH(st.PERIOD)                               >2
                            OR LENGTH(regexp_replace(st.PERIOD, '[-0-9]', '')) > 0
                            OR TRIM(st.PERIOD)                              LIKE ('-')
                            THEN
                            CASE
                                WHEN st.PERIOD         = ''
                                    OR st.PERIOD IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.PERIOD)>2
                                    THEN '006 - Data length is not valid'
                                WHEN LENGTH(regexp_replace(st.PERIOD, '[-0-9]', '')) > 0
                                    OR TRIM(st.PERIOD)                            LIKE ('-')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 10      AS COL_NUMBER
                  , st.YEAR AS col_value
                  , CASE
                        WHEN st.YEAR                                         = ''
                            OR st.YEAR                                 IS NULL
                            OR LENGTH(st.YEAR)                               >4
                            OR LENGTH(regexp_replace(st.YEAR, '[-0-9]', '')) > 0
                            OR TRIM(st.YEAR)                              LIKE ('-')
                            THEN
                            CASE
                                WHEN st.YEAR         = ''
                                    OR st.YEAR IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.YEAR)>4
                                    THEN '006 - Data length is not valid'
                                WHEN LENGTH(regexp_replace(st.YEAR, '[-0-9]', '')) > 0
                                    OR TRIM(st.YEAR)                            LIKE ('-')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 11           AS COL_NUMBER
                  , st.RA_FACTOR AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.RA_FACTOR, '[-0-9]', ''))        > 1
                            OR LENGTH( regexp_replace(st.RA_FACTOR, '[^a-z_A-Z ]', ''))>0
                            OR LENGTH(st.RA_FACTOR)                                    = 0
                            OR st.RA_FACTOR                                      IS NULL
                            OR TRIM(st.RA_FACTOR)                                   LIKE ('-')
                            THEN
                            CASE
                                WHEN st.RA_FACTOR         = ''
                                    OR st.RA_FACTOR IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN REGEXP_LIKE(TRIM(st.RA_FACTOR) , '^\d+(\.\d*)?$') != 'true'
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 12                             AS COL_NUMBER
                  , st.GROSS_ASSUMED_TREATY_NUMBER AS col_value
                  , CASE
                        WHEN LENGTH(st.GROSS_ASSUMED_TREATY_NUMBER)>9
                            THEN
                            CASE
                                WHEN LENGTH(st.GROSS_ASSUMED_TREATY_NUMBER)>9
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 13                              AS COL_NUMBER
                  , st.GROSS_ASSUMED_SECTION_NUMBER AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.GROSS_ASSUMED_SECTION_NUMBER, '[-0-9]', '')) > 0
                            OR TRIM(st.GROSS_ASSUMED_SECTION_NUMBER)                            LIKE ('-')
                            THEN
                            CASE
                                WHEN LENGTH(regexp_replace(st.GROSS_ASSUMED_SECTION_NUMBER, '[-0-9]', '')) > 0
                                    OR TRIM(st.GROSS_ASSUMED_SECTION_NUMBER)                            LIKE ('-')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 14                      AS COL_NUMBER
                  , st.REPORTING_BASIS_CODE AS col_value
                  , CASE
                        WHEN st.REPORTING_BASIS_CODE          = ''
                            OR st.REPORTING_BASIS_CODE  IS NULL
                            OR LENGTH(st.REPORTING_BASIS_CODE)>16
                            THEN
                            CASE
                                WHEN st.REPORTING_BASIS_CODE         = ''
                                    OR st.REPORTING_BASIS_CODE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.REPORTING_BASIS_CODE)>16
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 15              AS COL_NUMBER
                  , st.CLOSING_DATE AS col_value
                  , CASE
                        WHEN REPLACE(st.closing_date, chr(13), '')         = ''
                            OR REPLACE(st.closing_date, chr(13), '') IS NULL
                            THEN '002 - Mandatory data not mentioned'
                        WHEN LENGTH(REPLACE(st.closing_date, chr(13), '')) <> 10
                            THEN '006 - Data length is not valid'
                        WHEN LENGTH(regexp_replace(REPLACE(st.closing_date, chr(13), ''), '[0-9]', '')) != 2
                            OR regexp_replace(REPLACE(st.closing_date, chr(13), ''), '[!@#$%^&*+='
                                || chr(34)
                                || ':_{}|<>\;,.?]', '~') LIKE ('%~%')
                            THEN '004 - Data type is not valid'
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RD_FACTOR_PROJECTION st
            )
            aa
            JOIN
                DELIVERY_<env>."ERROR_MESSAGE" e
                ON
                    e.ERROR_MESSAGE_LABEL = aa.error_desc
    )
;