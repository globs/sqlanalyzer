SET SCHEMA STAGING_<env>;

DROP VIEW V_COL_ERROR_CHECK_EXPENSE_STAGING_VER_2; 

CREATE OR REPLACE VIEW V_COL_ERROR_CHECK_EXPENSE_STAGING_VER_2 AS (
SELECT
    aa.LINE_NUMBER,
    aa.col_number,
    aa.col_value,
    aa.error_desc,
    e.error_message_id
FROM
    (
    SELECT
        st1.LINE_NUMBER,
        st1.VERSION_YEAR_check,
        2 AS COL_NUMBER,
        st1.VERSION_YEAR AS col_value,
        CASE
            WHEN st1.VERSION_YEAR = '' THEN '002 - Mandatory data not mentioned'
            WHEN st1.VERSION_YEAR IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(regexp_replace(st1.VERSION_YEAR,
            '[-0-9]',
            '')) > 0
            OR TRIM(st1.VERSION_YEAR) LIKE ('-') THEN '004 - Data type is not valid'
            WHEN LENGTH(st1.VERSION_YEAR)>4 THEN '006 - Data length is not valid'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.VERSION_YEAR_check = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.VERSION_QUARTER_CHECK,
        3 AS COL_NUMBER,
        st1.VERSION_QUARTER AS col_value,
        CASE
            WHEN regexp_replace(VERSION_QUARTER,
            '[!#$%^&*+=' || chr(34)|| ':{}|<>\;,./?-]',
            '~') LIKE ('%~%') THEN '004 - Data type is not valid'
            WHEN st1.VERSION_QUARTER = '' THEN '002 - Mandatory data not mentioned'
            WHEN st1.VERSION_QUARTER IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(st1.VERSION_QUARTER)>2 THEN '006 - Data length is not valid'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.VERSION_QUARTER_CHECK = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.VERSION_TYPE_CHECK,
        4 AS COL_NUMBER,
        st1.VERSION_TYPE AS col_value,
        CASE
            WHEN regexp_replace(VERSION_TYPE,
            '[!#$%^&*+=' || chr(34)|| ':{}|<>\;,./?-]',
            '~') LIKE ('%~%') THEN '004 - Data type is not valid'
            WHEN st1.VERSION_TYPE = '' THEN '002 - Mandatory data not mentioned'
            WHEN st1.VERSION_TYPE IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(st1.VERSION_TYPE)>7 THEN '006 - Data length is not valid'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.VERSION_TYPE_CHECK = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.PRODUCT_LINE_CHECK,
        5 AS COL_NUMBER,
        st1.PRODUCT_LINE AS col_value,
        CASE
            WHEN regexp_replace(PRODUCT_LINE,
            '[!#$%^&*+=' || chr(34)|| ':{}|<>\;,./?-]',
            '~') LIKE ('%~%') THEN '004 - Data type is not valid'
            WHEN st1.PRODUCT_LINE = '' THEN '002 - Mandatory data not mentioned'
            WHEN st1.PRODUCT_LINE IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(st1.PRODUCT_LINE)>4 THEN '006 - Data length is not valid'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.PRODUCT_LINE_CHECK = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.SEGMENT_CODE_check,
        6 AS COL_NUMBER,
        st1.SEGMENT_CODE AS col_value,
        CASE
            WHEN st1.SEGMENT_CODE = '' THEN '002 - Mandatory data not mentioned'
            WHEN st1.SEGMENT_CODE IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(regexp_replace(st1.SEGMENT_CODE,
            '[-0-9]',
            '')) > 0
            OR TRIM(st1.SEGMENT_CODE) LIKE ('-') THEN '004 - Data type is not valid'
            WHEN LENGTH(st1.SEGMENT_CODE)>3 THEN '006 - Data length is not valid'
            WHEN LENGTH(regexp_replace(st1.SEGMENT_CODE,
            '[-0-9]',
            '')) > 0
            OR TRIM(st1.SEGMENT_CODE) LIKE ('-') THEN '004 - Data type is not valid'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.SEGMENT_CODE_check = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.SEGMENT_LABEL_CHECK,
        7 AS COL_NUMBER,
        st1.SEGMENT_LABEL AS col_value,
        CASE
            WHEN regexp_replace(SEGMENT_LABEL,
            '[!#$%^&*+=' || chr(34)|| ':{}|<>\;,./?-]',
            '~') LIKE ('%~%') THEN '004 - Data type is not valid'
            WHEN st1.SEGMENT_LABEL = '' THEN '002 - Mandatory data not mentioned'
            WHEN st1.SEGMENT_LABEL IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(st1.SEGMENT_LABEL)>64 THEN '006 - Data length is not valid'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.SEGMENT_LABEL_CHECK = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.EXPENSE_RATIO_TYPE_CHECK,
        8 AS COL_NUMBER,
        st1.EXPENSE_RATIO_TYPE AS col_value,
        CASE
            WHEN regexp_replace(EXPENSE_RATIO_TYPE,
            '[!#$%^&*+=' || chr(34)|| ':{}|<>\;,./?-]',
            '~') LIKE ('%~%') THEN '004 - Data type is not valid'
            WHEN st1.EXPENSE_RATIO_TYPE = '' THEN '002 - Mandatory data not mentioned'
            WHEN st1.EXPENSE_RATIO_TYPE IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(st1.EXPENSE_RATIO_TYPE)>11 THEN '006 - Data length is not valid'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.EXPENSE_RATIO_TYPE_CHECK = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.CURRENCY_CODE_CHECK,
        9 AS COL_NUMBER,
        st1.CURRENCY_CODE AS col_value,
        CASE
            WHEN regexp_replace(CURRENCY_CODE,
            '[!#$%^&*+=' || chr(34)|| ':{}|<>\;,./?-]',
            '~') LIKE ('%~%') THEN '004 - Data type is not valid'
            WHEN st1.CURRENCY_CODE = '' THEN '002 - Mandatory data not mentioned'
            WHEN st1.CURRENCY_CODE IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN LENGTH(st1.CURRENCY_CODE)>3 THEN '006 - Data length is not valid'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.CURRENCY_CODE_CHECK = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.ALLOCATED_AMOUNT_check,
        10 AS COL_NUMBER,
        st1.ALLOCATED_AMOUNT AS COL_VALUE,
        CASE
            WHEN st1.ALLOCATED_AMOUNT IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN st1.ALLOCATED_AMOUNT = '' THEN '002 - Mandatory data not mentioned'
            WHEN REGEXP_LIKE(TRIM(ALLOCATED_AMOUNT) ,
            '^\d+(\.\d*)?$') != 'true' THEN '004 - Data type is not valid'
            WHEN LENGTH(REPLACE(st1.ALLOCATED_AMOUNT, chr(13), ''))= 0 THEN '002 - Mandatory data not mentioned'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.ALLOCATED_AMOUNT_check = 'error'
UNION
    SELECT
        st1.LINE_NUMBER,
        st1.EXPENSE_RATIO_check,
        11 AS COL_NUMBER,
        st1.EXPENSE_RATIO AS COL_VALUE,
        CASE
            WHEN st1.EXPENSE_RATIO IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN st1.EXPENSE_RATIO = '' THEN '002 - Mandatory data not mentioned'
            WHEN REGEXP_LIKE(TRIM(EXPENSE_RATIO) ,
            '^\d+(\.\d*)?$') != 'true' THEN '004 - Data type is not valid'
            WHEN LENGTH(REPLACE(st1.EXPENSE_RATIO, chr(13), ''))= 0 THEN '002 - Mandatory data not mentioned'
            ELSE '-1'
        END AS error_desc
    FROM
        V_ERROR_EXPENSE_STAGING_VER_2 st1
    WHERE
        st1.EXPENSE_RATIO_check = 'error' ) aa
JOIN DELIVERY_<env>.ERROR_MESSAGE e ON
    e.ERROR_MESSAGE_LABEL = aa.error_desc );	


