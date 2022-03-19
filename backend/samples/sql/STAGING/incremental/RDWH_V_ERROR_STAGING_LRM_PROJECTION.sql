SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_STAGING_LRM_PROJECTION;

CREATE OR REPLACE VIEW STAGING_<env>.V_ERROR_STAGING_LRM_PROJECTION AS
    (
    (
        SELECT
            aa.line_number
          , aa.col_number
          , aa.col_value
          , aa.error_desc
          , e.error_message_id
        FROM
            (
                SELECT
                    st.LINE_NUMBER
                  , 1                AS COL_NUMBER
                  , st.TREATY_NUMBER AS col_value
                  , CASE
                        WHEN st.TREATY_NUMBER           = ''
                            OR st.TREATY_NUMBER   IS NULL
                            OR LENGTH(st.TREATY_NUMBER)<> 9
                            OR regexp_replace(st.TREATY_NUMBER, '[!@#$%^&*+='
                                || chr(34)
                                || ':{}|<>\;,/?]', '~') LIKE ('%~%')
                            THEN
                            CASE
                                WHEN st.TREATY_NUMBER         = ''
                                    OR st.TREATY_NUMBER IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.TREATY_NUMBER)<> 9
                                    THEN '006 - Data length is not valid'
                                WHEN regexp_replace(st.TREATY_NUMBER, '[!@#$%^&*+='
                                        || chr(34)
                                        || ':_{}|<>\;,./?-]', '~') LIKE ('%~%')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 2                 AS COL_NUMBER
                  , st.SECTION_NUMBER AS col_value
                  , CASE
                        WHEN st.SECTION_NUMBER                                         = ''
                            OR st.SECTION_NUMBER                                 IS NULL
                            OR LENGTH(regexp_replace(st.SECTION_NUMBER, '[-0-9]', '')) > 0
                            OR TRIM(st.SECTION_NUMBER)                              LIKE ('-%')
                            THEN
                            CASE
                                WHEN st.SECTION_NUMBER         = ''
                                    OR st.SECTION_NUMBER IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(regexp_replace(st.SECTION_NUMBER, '[-0-9]', '')) > 0
                                    OR TRIM(st.SECTION_NUMBER)                            LIKE ('-%')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 3                           AS COL_NUMBER
                  , st.POLICY_UNDERWRITING_YEAR AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.POLICY_UNDERWRITING_YEAR, '[-0-9]', '')) > 0
                            OR TRIM(st.POLICY_UNDERWRITING_YEAR)                            LIKE ('-%')
                            THEN
                            CASE
                                WHEN LENGTH(regexp_replace(st.POLICY_UNDERWRITING_YEAR, '[-0-9]', '')) > 0
                                    OR TRIM(st.POLICY_UNDERWRITING_YEAR)                            LIKE ('-%')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 4                       AS COL_NUMBER
                  , st.REPORTING_BASIS_CODE AS col_value
                  , CASE
                        WHEN st.REPORTING_BASIS_CODE          = ''
                            OR st.REPORTING_BASIS_CODE  IS NULL
                            OR LENGTH(st.REPORTING_BASIS_CODE)>50
                            OR regexp_replace(st.REPORTING_BASIS_CODE, '[!@#$%^&*+='
                                || chr(34)
                                || ':{}|<>\;,/?]', '~') LIKE ('%~%')
                            THEN
                            CASE
                                WHEN st.REPORTING_BASIS_CODE         = ''
                                    OR st.REPORTING_BASIS_CODE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.REPORTING_BASIS_CODE)>50
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 5                         AS COL_NUMBER
                  , st.Business_Maturity_Code AS col_value
                  , CASE
                        WHEN st.Business_Maturity_Code          = ''
                            OR st.Business_Maturity_Code  IS NULL
                            OR LENGTH(st.Business_Maturity_Code)>50
                            OR regexp_replace(st.Business_Maturity_Code, '[!@#$%^&*+='
                                || chr(34)
                                || ':{}|<>\;,/?]', '~') LIKE ('%~%')
                            THEN
                            CASE
                                WHEN st.Business_Maturity_Code         = ''
                                    OR st.Business_Maturity_Code IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.Business_Maturity_Code)>50
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 6               AS COL_NUMBER
                  , st.CLOSING_DATE AS col_value
                  , CASE
                        WHEN st.CLOSING_DATE          = ''
                            OR st.CLOSING_DATE  IS NULL
                            OR LENGTH(st.CLOSING_DATE)>10
                            THEN
                            CASE
                                WHEN st.CLOSING_DATE         = ''
                                    OR st.CLOSING_DATE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.CLOSING_DATE)>10
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 7                                AS COL_NUMBER
                  , st.GROSS_ASSUMED_CONTRACT_NUMBER AS col_value
                  , CASE
                        WHEN LENGTH(st.GROSS_ASSUMED_CONTRACT_NUMBER)<> 9
                            OR regexp_replace(st.GROSS_ASSUMED_CONTRACT_NUMBER, '[!@#$%^&*+='
                                || chr(34)
                                || ':{}|<>\;,/?]', '~') LIKE ('%~%')
                            THEN
                            CASE
                                WHEN LENGTH(st.GROSS_ASSUMED_CONTRACT_NUMBER)!= 0
                                    THEN
                                    CASE
                                        WHEN LENGTH(st.GROSS_ASSUMED_CONTRACT_NUMBER)<> 9
                                            THEN '006 - Data length is not valid'
                                        WHEN regexp_replace(st.GROSS_ASSUMED_CONTRACT_NUMBER, '[!@#$%^&*+='
                                                || chr(34)
                                                || ':_{}|<>\;,./?-]', '~') LIKE ('%~%')
                                            THEN '004 - Data type is not valid'
                                            ELSE '-1'
                                    END
                                    ELSE 'success'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 8                               AS COL_NUMBER
                  , st.GROSS_ASSUMED_SECTION_NUMBER AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.GROSS_ASSUMED_SECTION_NUMBER, '[-0-9]', '')) > 0
                            OR TRIM(st.GROSS_ASSUMED_SECTION_NUMBER)                            LIKE ('-%')
                            THEN
                            CASE
                                WHEN LENGTH(regexp_replace(st.GROSS_ASSUMED_SECTION_NUMBER, '[-0-9]', '')) > 0
                                    OR TRIM(st.GROSS_ASSUMED_SECTION_NUMBER)                            LIKE ('-%')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 9                AS COL_NUMBER
                  , st.CURRENCY_CODE AS col_value
                  , CASE
                        WHEN st.CURRENCY_CODE           = ''
                            OR st.CURRENCY_CODE   IS NULL
                            OR LENGTH(st.CURRENCY_CODE)<> 3
                            OR regexp_replace(st.CURRENCY_CODE, '[!@#$%^&*+='
                                || chr(34)
                                || ':{}|<>\;,/?]', '~') LIKE ('%~%')
                            THEN
                            CASE
                                WHEN st.CURRENCY_CODE         = ''
                                    OR st.CURRENCY_CODE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.CURRENCY_CODE)<> 3
                                    THEN '006 - Data length is not valid'
                                WHEN regexp_replace(st.CURRENCY_CODE, '[!@#$%^&*+='
                                        || chr(34)
                                        || ':_{}|<>\;,./?-]', '~') LIKE ('%~%')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 10                        AS COL_NUMBER
                  , st.CSM_CASHFLOW_LEGS_CODE AS col_value
                  , CASE
                        WHEN st.CSM_CASHFLOW_LEGS_CODE          = ''
                            OR st.CSM_CASHFLOW_LEGS_CODE  IS NULL
                            OR LENGTH(st.CSM_CASHFLOW_LEGS_CODE)> 100
                            OR regexp_replace(st.CSM_CASHFLOW_LEGS_CODE, '[!@#$%^&*+='
                                || chr(34)
                                || ':{}|<>\;,/?]', '~') LIKE ('%~%')
                            THEN
                            CASE
                                WHEN st.CSM_CASHFLOW_LEGS_CODE         = ''
                                    OR st.CSM_CASHFLOW_LEGS_CODE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.CSM_CASHFLOW_LEGS_CODE)> 100
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 11                    AS COL_NUMBER
                  , st.SCENARIO_TYPE_CODE AS col_value
                  , CASE
                        WHEN st.SCENARIO_TYPE_CODE          = ''
                            OR st.SCENARIO_TYPE_CODE  IS NULL
                            OR LENGTH(st.SCENARIO_TYPE_CODE)> 32
                            OR regexp_replace(st.SCENARIO_TYPE_CODE, '[!@#$%^&*+='
                                || chr(34)
                                || ':{}|<>\;,/?]', '~') LIKE ('%~%')
                            THEN
                            CASE
                                WHEN st.SCENARIO_TYPE_CODE         = ''
                                    OR st.SCENARIO_TYPE_CODE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.SCENARIO_TYPE_CODE)> 32
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 12                    AS COL_NUMBER
                  , st.Scenario_Parameter AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.Scenario_Parameter, '[-0-9]', ''))        > 1
                            OR LENGTH( regexp_replace(st.Scenario_Parameter, '[^a-z_A-Z ]', ''))>0
                            OR LENGTH(st.Scenario_Parameter)                                    = 0
                            OR st.Scenario_Parameter                                      IS NULL
                            OR TRIM(st.Scenario_Parameter)                                   LIKE ('-')
                            THEN
                            CASE
                                WHEN st.Scenario_Parameter         = ''
                                    OR st.Scenario_Parameter IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN REGEXP_LIKE(TRIM(st.Scenario_Parameter) , '^\d+(\.\d*)?$') != 'true'
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 13                        AS COL_NUMBER
                  , st.LEVEL_OF_ANALYSIS_CODE AS col_value
                  , CASE
                        WHEN st.LEVEL_OF_ANALYSIS_CODE          = ''
                            OR st.LEVEL_OF_ANALYSIS_CODE  IS NULL
                            OR LENGTH(st.LEVEL_OF_ANALYSIS_CODE)> 100
                            OR regexp_replace(st.LEVEL_OF_ANALYSIS_CODE, '[!@#$%^&*+='
                                || chr(34)
                                || ':{}|<>\;,/?]', '~') LIKE ('%~%')
                            THEN
                            CASE
                                WHEN st.LEVEL_OF_ANALYSIS_CODE         = ''
                                    OR st.LEVEL_OF_ANALYSIS_CODE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.LEVEL_OF_ANALYSIS_CODE)> 100
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 14           AS COL_NUMBER
                  , st.PERIOD_ID AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.PERIOD_ID, '[-0-9]', '')) > 0
                            OR TRIM(st.PERIOD_ID)                            LIKE ('-%')
                            THEN
                            CASE
                                WHEN LENGTH(regexp_replace(st.PERIOD_ID, '[-0-9]', '')) > 0
                                    OR TRIM(st.PERIOD_ID)                            LIKE ('-%')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 15        AS COL_NUMBER
                  , st.PERIOD AS col_value
                  , CASE WHEN LENGTH(regexp_replace(st.PERIOD, '[-0-9]', '')) > 0
                            OR TRIM(st.PERIOD)                           LIKE ('-%')
                            THEN
                            CASE
                                WHEN LENGTH(regexp_replace(st.PERIOD, '[-0-9]', '')) > 0
                                    OR TRIM(st.PERIOD)                            LIKE ('-%')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 16      AS COL_NUMBER
                  , st.YEAR AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.YEAR, '[-0-9]', '')) > 0
                            OR TRIM(st.YEAR)                            LIKE ('-%')
                            THEN
                            CASE
                                WHEN LENGTH(regexp_replace(st.YEAR, '[-0-9]', '')) > 0
                                    OR TRIM(st.YEAR)                            LIKE ('-%')
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
                UNION ALL
                SELECT
                    st.LINE_NUMBER
                  , 17       AS COL_NUMBER
                  , st.VALUE AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.VALUE, '[-0-9]', ''))        > 1
                            OR LENGTH( regexp_replace(st.VALUE, '[^a-z_A-Z ]', ''))>0
                            OR LENGTH(st.VALUE)                                    = 0
                            OR st.VALUE                                      IS NULL
                            OR TRIM(st.VALUE)                                   LIKE ('-')
                            THEN
                            CASE
                                WHEN st.VALUE         = ''
                                    OR st.VALUE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN REGEXP_LIKE(TRIM(st.VALUE) , '^\d+(\.\d*)?$') != 'true'
                                    THEN '004 - Data type is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_<env>.STAGING_LRM_PROJECTION st
            )
            aa
            JOIN
                DELIVERY_<env>."ERROR_MESSAGE" e
                ON
                    e.ERROR_MESSAGE_LABEL = aa.error_desc
    )
    )
;