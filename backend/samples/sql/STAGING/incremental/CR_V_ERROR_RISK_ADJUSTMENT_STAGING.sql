SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_RISK_ADJUSTMENT_STAGING;

CREATE OR REPLACE VIEW V_ERROR_RISK_ADJUSTMENT_STAGING AS
    (
        SELECT
            aa.LINE_NUMBER
          , aa.COL_NUMBER
          , aa.COL_VALUE
          , aa.error_desc
          , e.error_message_id
        FROM
            (
                SELECT
                    LINE_NUMBER
                  , 2           AS COL_NUMBER
                  , SUBSIDIARY  AS COL_VALUE
                  , CASE
                        WHEN LENGTH(regexp_replace(SUBSIDIARY, '[-0-9]', '')) > 0
                            OR TRIM(SUBSIDIARY)                            LIKE ('-')
                            THEN '004 - Data type is not valid'
                        WHEN LENGTH(SUBSIDIARY)>2
                            THEN '006 - Data length is not valid'
                        WHEN (
                                (
                                    SUBSIDIARY          = ''
                                    OR SUBSIDIARY IS NULL
                                )
                                AND NATURE        <> 'F'
                                AND LENGTH(NATURE)<> 0
                            )
                            OR (
                                (
                                    SUBSIDIARY          = ''
                                    OR SUBSIDIARY IS NULL
                                )
                                AND LENGTH(NATURE)= 0
                                AND loB_N1 NOT IN('3021', '3011')
                            )
                            OR (
                                LENGTH(SUBSIDIARY)    = 0
                                AND LENGTH(SUBLEDGER)<> 0
                                AND LENGTH(NATURE)    = 0
                            )
                            THEN '002 - Mandatory data not mentioned'
                            ELSE '-1'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT
                UNION
                SELECT
                    LINE_NUMBER
                  , 3             AS COL_NUMBER
                  , SUBLEDGER AS COL_VALUE
                  , CASE
                        WHEN LENGTH(regexp_replace(SUBLEDGER, '[-0-9]', '')) > 0
                            OR TRIM(SUBLEDGER)                            LIKE ('-')
                            THEN '004 - Data type is not valid'
                        WHEN LENGTH(SUBLEDGER)>2
                            THEN '006 - Data length is not valid'
                        WHEN (
                                (
                                    SUBLEDGER          = ''
                                    OR SUBLEDGER IS NULL
                                )
                                AND NATURE        <> 'F'
                                AND LENGTH(NATURE)<> 0
                            )
                            OR (
                                (
                                   (SUBLEDGER          = ''
                                    OR SUBLEDGER IS NULL)
                                    AND LENGTH(NATURE) = 0
                                    AND loB_N1 NOT IN('3021', '3011')
                                )
                            )
                            OR (
                                LENGTH(subsidiary)    = 0
                                AND LENGTH(subledger)<> 0
                                AND LENGTH(nature)    = 0
                            )
                            THEN '002 - Mandatory data not mentioned'
                            ELSE '-1'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT
                UNION
                SELECT
                    LINE_NUMBER
                  , 4          AS COL_NUMBER
                  , LOB_N1 AS COL_VALUE
                  , CASE
                        WHEN regexp_replace(LOB_N1, '[!@#$%^&*+='
                                || chr(34)
                                || ':_{}|<>\;,./?]', '~') LIKE ('%~%')
                            THEN '004 - Data type is not valid'
                        WHEN LOB_N1 = ''
                            THEN '002 - Mandatory data not mentioned'
                        WHEN LOB_N1 IS NULL
                            THEN '002 - Mandatory data not mentioned'
                        WHEN LENGTH(LOB_N1)>4
                            THEN '006 - Data length is not valid'
                            ELSE '-1'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT 
                UNION
                SELECT
                    LINE_NUMBER
                  , 6        AS COL_NUMBER
                  , NORM AS COL_VALUE
                  , CASE
                        WHEN regexp_replace(NORM, '[!@#$%^&*+='
                                || chr(34)
                                || ':_{}|<>\;,./?]', '~') LIKE ('%~%')
                            THEN '004 - Data type is not valid'
                        WHEN LENGTH(NORM)>5
                            THEN '006 - Data length is not valid'
                        WHEN NORM = ''
                            THEN '002 - Mandatory data not mentioned'
                        WHEN NORM IS NULL
                            THEN '002 - Mandatory data not mentioned'
                            ELSE '-1'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT
                UNION
                SELECT
                    LINE_NUMBER
                  , 5          AS COL_NUMBER
                  , NATURE AS COL_VALUE
                  , CASE
                        WHEN regexp_replace(NATURE, '[!@#$%^&*+='
                                || chr(34)
                                || ':_{}|<>\;,./?]', '~') LIKE ('%~%')
                            THEN '004 - Data type is not valid'
                        WHEN (
                                NATURE          = ''
                                OR NATURE IS NULL
                            )
                            AND (
                                LENGTH(subsidiary)  <> 0
                                OR LENGTH(subledger)<> 0
                            )
                            THEN '002 - Mandatory data not mentioned'
                        WHEN LENGTH(NATURE)>1
                            THEN '006 - Data length is not valid'
                            ELSE '-1'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT
                UNION
                SELECT
                    LINE_NUMBER
                  , 7               AS COL_NUMBER
                  , DOMAIN_CODE AS COL_VALUE
                  , CASE
                        WHEN regexp_replace(DOMAIN_CODE, '[!@#$%^&*+='
                                || chr(34)
                                || ':_{}|<>\;,./?]', '~') LIKE ('%~%')
                            THEN '004 - Data type is not valid'
                        WHEN LENGTH(DOMAIN_CODE)>7
                            THEN '006 - Data length is not valid'
                        WHEN DOMAIN_CODE = ''
                            THEN '002 - Mandatory data not mentioned'
                        WHEN DOMAIN_CODE IS NULL
                            THEN '002 - Mandatory data not mentioned'
                            ELSE '-1'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT
                UNION
                SELECT
                    LINE_NUMBER
                  , 8           AS COL_NUMBER
                  , PREMIUM AS COL_VALUE
                  , CASE
                        WHEN PREMIUM = ''
                            THEN '002 - Mandatory data not mentioned'
                        WHEN PREMIUM IS NULL
                            THEN '002 - Mandatory data not mentioned'
                        WHEN REGEXP_LIKE(TRIM(PREMIUM) , '^\d+(\.\d*)?$') != 'true'
                            THEN '004 - Data type is not valid'
                        WHEN LENGTH(REPLACE(PREMIUM, chr(13), ''))= 0
                            THEN '002 - Mandatory data not mentioned'
                            ELSE '-1'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT
                UNION
                SELECT
                    LINE_NUMBER
                  , 9            AS COL_NUMBER
                  , RESERVES AS COL_VALUE
                  , CASE
                        WHEN RESERVES IS NULL
                            THEN '002 - Mandatory data not mentioned'
                        WHEN RESERVES = ''
                            THEN '002 - Mandatory data not mentioned'
                        WHEN REGEXP_LIKE(TRIM(RESERVES) , '^\d+(\.\d*)?$') != 'true'
                            THEN '004 - Data type is not valid'
                        WHEN RESERVES = ''
                            THEN '002 - Mandatory data not mentioned'
                        WHEN LENGTH(REPLACE(RESERVES, chr(13), ''))= 0
                            THEN '002 - Mandatory data not mentioned'
                            ELSE '-1'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT
            )
            aa
            JOIN
                DELIVERY_<env>."ERROR_MESSAGE" e
                ON
                    e.ERROR_MESSAGE_LABEL = aa.error_desc
    )
;



	