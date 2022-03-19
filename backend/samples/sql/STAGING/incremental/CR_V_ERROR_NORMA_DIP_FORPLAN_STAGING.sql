SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_NORMA_DIP_FORPLAN_STAGING;

CREATE OR REPLACE VIEW V_ERROR_NORMA_DIP_FORPLAN_STAGING AS
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
                  , 2         AS COL_NUMBER
                  , st.LOB_N1 AS col_value
                  , CASE
                        WHEN LENGTH(st.LOB_N1) >4
                            OR st.LOB_N1 IS NULL
                            OR st.LOB_N1       = ''
                            THEN
                            CASE
                                WHEN st.LOB_N1 IS NULL
                                    OR st.LOB_N1     = ''
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.LOB_N1)>4
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT_FORPLAN st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 3         AS COL_NUMBER
                  , st.NATURE AS col_value
                  , CASE
                        WHEN st.NATURE          = ''
                            OR st.NATURE  IS NULL
                            OR LENGTH(st.NATURE)>1
                            THEN
                            CASE
                                WHEN st.NATURE         = ''
                                    OR st.NATURE IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.NATURE)>1
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT_FORPLAN st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 4       AS COL_NUMBER
                  , st.NORM AS col_value
                  , CASE
                        WHEN st.NORM          = ''
                            OR st.NORM  IS NULL
                            OR LENGTH(st.NORM)>5
                            THEN
                            CASE
                                WHEN st.NORM         = ''
                                    OR st.NORM IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN LENGTH(st.NORM)>5
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT_FORPLAN st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 5          AS COL_NUMBER
                  , st.PREMIUM AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.PREMIUM, '[-0-9]', ''))        > 1
                            OR LENGTH( regexp_replace(st.PREMIUM, '[^a-z_A-Z ]', ''))>0
                            OR LENGTH(st.PREMIUM)                                    = 0
                            OR st.PREMIUM                                      IS NULL
                            OR LENGTH(st.PREMIUM)                                    >53
                            OR TRIM(st.PREMIUM)                                   LIKE ('-')
                            THEN
                            CASE
                                WHEN st.PREMIUM         = ''
                                    OR st.PREMIUM IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN REGEXP_LIKE(TRIM(st.PREMIUM) , '^\d+(\.\d*)?$') != 'true'
                                    THEN '004 - Data type is not valid'
                                WHEN LENGTH(st.PREMIUM)>53
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT_FORPLAN st
                UNION
                SELECT
                    st.LINE_NUMBER
                  , 6           AS COL_NUMBER
                  , st.RESERVES AS col_value
                  , CASE
                        WHEN LENGTH(regexp_replace(st.RESERVES, '[-0-9]', ''))        > 1
                            OR LENGTH( regexp_replace(st.RESERVES, '[^a-z_A-Z ]', ''))>0
                            OR LENGTH(st.RESERVES)                                    = 0
                            OR st.RESERVES                                      IS NULL
                            OR LENGTH(st.RESERVES)                                    >53
                            OR TRIM(st.RESERVES)                                   LIKE ('-')
                            THEN
                            CASE
                                WHEN st.RESERVES         = ''
                                    OR st.RESERVES IS NULL
                                    THEN '002 - Mandatory data not mentioned'
                                WHEN REGEXP_LIKE(TRIM(st.RESERVES) , '^\d+(\.\d*)?$') != 'true'
                                    THEN '004 - Data type is not valid'
                                WHEN LENGTH(st.RESERVES)>53
                                    THEN '006 - Data length is not valid'
                                    ELSE '-1'
                            END
                            ELSE 'success'
                    END AS error_desc
                FROM
                    STAGING_RISK_ADJUSTMENT_FORPLAN st
            )
            aa
            JOIN
                DELIVERY_<env>.ERROR_MESSAGE e
                ON
                    e.ERROR_MESSAGE_LABEL = aa.error_desc
    )
;
