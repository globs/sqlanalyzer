SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_YIELD_CURVE_STAGING;

CREATE OR REPLACE VIEW V_ERROR_YIELD_CURVE_STAGING AS (
SELECT
    aa.LINE_NUMBER,
    aa.col_number ,
    aa.col_value ,
    aa.error_desc ,
    e.error_message_id
FROM
    (
    SELECT
        st.LINE_NUMBER ,
        2 AS COL_NUMBER ,
        st.CURRENCY_CODE AS col_value,
        CASE
            WHEN st.CURRENCY_CODE = ''
            OR st.CURRENCY_CODE IS NULL
            OR LENGTH(st.CURRENCY_CODE)>3
            OR regexp_replace(st.CURRENCY_CODE,
            '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN st.CURRENCY_CODE = ''
                OR st.CURRENCY_CODE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN regexp_replace(st.CURRENCY_CODE,
                '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.CURRENCY_CODE)>3 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS error_desc
    FROM
        STAGING_YIELD_CURVE st
UNION
    SELECT
        st.LINE_NUMBER ,
        3 AS COL_NUMBER ,
        st.NORM_CODE AS col_value,
        CASE
            WHEN st.NORM_CODE = ''
            OR st.NORM_CODE IS NULL
            OR LENGTH(st.NORM_CODE)>4
            OR regexp_replace(st.NORM_CODE,
            '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN st.NORM_CODE = ''
                OR st.NORM_CODE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN regexp_replace(st.NORM_CODE,
                '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.NORM_CODE)>4 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS error_desc
    FROM
        STAGING_YIELD_CURVE st
UNION
    SELECT
        st.LINE_NUMBER ,
        4 AS COL_NUMBER ,
        st.SEGMENT_CODE AS col_value,
        CASE
            WHEN LENGTH(st.SEGMENT_CODE)>16
            OR regexp_replace(st.SEGMENT_CODE,
            '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN regexp_replace(st.SEGMENT_CODE,
                '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.SEGMENT_CODE)>16 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS error_desc
    FROM
        STAGING_YIELD_CURVE st
UNION
    SELECT
        st.LINE_NUMBER ,
        5 AS COL_NUMBER ,
        st.CURVE_TYPE AS col_value,
        CASE
            WHEN st.CURVE_TYPE = ''
            OR st.CURVE_TYPE IS NULL
            OR LENGTH(st.CURVE_TYPE)>4
            OR regexp_replace(st.CURVE_TYPE,
            '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN st.CURVE_TYPE = ''
                OR st.CURVE_TYPE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN regexp_replace(st.CURVE_TYPE,
                '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.CURVE_TYPE)>4 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS error_desc
    FROM
        STAGING_YIELD_CURVE st
UNION
    SELECT
        st.LINE_NUMBER ,
        6 AS COL_NUMBER ,
        st.MATURITY_TYPE AS col_value,
        CASE
            WHEN st.MATURITY_TYPE = ''
            OR st.MATURITY_TYPE IS NULL
            OR LENGTH(st.MATURITY_TYPE)>1
            OR regexp_replace(st.MATURITY_TYPE,
            '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN st.MATURITY_TYPE = ''
                OR st.MATURITY_TYPE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN regexp_replace(st.MATURITY_TYPE,
                '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.MATURITY_TYPE)>1 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS error_desc
    FROM
        STAGING_YIELD_CURVE st
UNION
    SELECT
        st.LINE_NUMBER ,
        7 AS COL_NUMBER ,
        st.MATURITY AS col_value,
        CASE
            WHEN LENGTH(regexp_replace(st.MATURITY,
            '[-0-9]',
            '')) > 0
            OR regexp_replace(st.MATURITY,
            '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(st.MATURITY)= 0
            OR st.MATURITY IS NULL
            OR TRIM(st.MATURITY) LIKE ('-') THEN
            CASE
                WHEN st.MATURITY = ''
                OR st.MATURITY IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(st.MATURITY) ,
                '^\d+(\.\d*)?$') != 'true' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS error_desc
    FROM
        STAGING_YIELD_CURVE st
UNION
    SELECT
        st.LINE_NUMBER ,
        8 AS COL_NUMBER ,
        st.RATE AS col_value,
        CASE
            WHEN LENGTH(regexp_replace(st.RATE,
            '[-0-9]',
            '')) > 1
            OR regexp_replace(st.RATE,
            '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%')
            OR LENGTH(st.RATE)= 0
            OR st.RATE IS NULL
            OR TRIM(st.RATE) LIKE ('-') THEN
            CASE
                WHEN st.RATE = ''
                OR st.RATE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN REGEXP_LIKE(TRIM(st.RATE) ,
                '^\d+(\.\d*)?$') != 'true' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS error_desc
    FROM
        STAGING_YIELD_CURVE st
UNION
    SELECT
        st.LINE_NUMBER ,
        9 AS COL_NUMBER ,
        st.ID AS col_value,
        CASE
            WHEN LENGTH(st.ID)>32
            OR regexp_replace(st.ID,
            '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
            '~') LIKE ('%~%') THEN
            CASE
                WHEN regexp_replace(st.ID,
                '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]',
                '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.ID)>32 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS error_desc
    FROM
        STAGING_YIELD_CURVE st ) aa
JOIN DELIVERY_<env>."ERROR_MESSAGE" e ON
    e.ERROR_MESSAGE_LABEL = aa.error_desc );
	
	

