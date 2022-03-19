SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_EXPENSE_STAGING_VER_2;

CREATE OR REPLACE VIEW V_ERROR_EXPENSE_STAGING_VER_2 AS (
SELECT
    LINE_NUMBER,
    CASE
        WHEN VERSION_YEAR = ''
        OR VERSION_YEAR IS NULL
        OR LENGTH(VERSION_YEAR)>4
        OR LENGTH(regexp_replace(VERSION_YEAR,
        '[-0-9]',
        '')) > 0
        OR TRIM(VERSION_YEAR) LIKE ('-') THEN 'error'
        ELSE 'success'
    END AS VERSION_YEAR_check,
    VERSION_YEAR,
    CASE
        WHEN VERSION_QUARTER = ''
        OR VERSION_QUARTER IS NULL
        OR regexp_replace(VERSION_QUARTER,
        '[!#$%^&*+=' || chr(34)|| '_{}|<>\;,./?-]',
        '~') LIKE ('%~%')
        OR LENGTH(VERSION_QUARTER)>2 THEN 'error'
        ELSE 'success'
    END AS VERSION_QUARTER_check,
    VERSION_QUARTER,
    CASE
        WHEN VERSION_TYPE = ''
        OR VERSION_TYPE IS NULL
        OR regexp_replace(VERSION_TYPE,
        '[!#$%^&*+=' || chr(34)|| '_{}|<>\;,./?-]',
        '~') LIKE ('%~%')
        OR LENGTH(VERSION_TYPE)>7 THEN 'error'
        ELSE 'success'
    END AS VERSION_TYPE_check,
    VERSION_TYPE,
    CASE
        WHEN PRODUCT_LINE = ''
        OR PRODUCT_LINE IS NULL
        OR regexp_replace(PRODUCT_LINE,
        '[!#$%^&*+=' || chr(34)|| '_{}|<>\;,./?-]',
        '~') LIKE ('%~%')
        OR LENGTH(PRODUCT_LINE)>4 THEN 'error'
        ELSE 'success'
    END AS PRODUCT_LINE_check,
    PRODUCT_LINE,
    CASE
        WHEN SEGMENT_CODE = ''
        OR SEGMENT_CODE IS NULL
        OR LENGTH(SEGMENT_CODE)>3
        OR LENGTH(regexp_replace(SEGMENT_CODE,
        '[-0-9]',
        '')) > 0
        OR TRIM(SEGMENT_CODE) LIKE ('-') THEN 'error'
        ELSE 'success'
    END AS SEGMENT_CODE_check,
    SEGMENT_CODE,
    CASE
        WHEN SEGMENT_LABEL = ''
        OR SEGMENT_LABEL IS NULL
        OR regexp_replace(SEGMENT_LABEL,
        '[!#$%^&*+=' || chr(34)|| '_{}|<>\;,./?-]',
        '~') LIKE ('%~%')
        OR LENGTH(SEGMENT_LABEL)>64 THEN 'error'
        ELSE 'success'
    END AS SEGMENT_LABEL_check,
    SEGMENT_LABEL,
    CASE
        WHEN EXPENSE_RATIO_TYPE = ''
        OR EXPENSE_RATIO_TYPE IS NULL
        OR regexp_replace(EXPENSE_RATIO_TYPE,
        '[!#$%^&*+=' || chr(34)|| '_{}|<>\;,./?-]',
        '~') LIKE ('%~%')
        OR LENGTH(EXPENSE_RATIO_TYPE)>11 THEN 'error'
        ELSE 'success'
    END AS EXPENSE_RATIO_TYPE_check,
    EXPENSE_RATIO_TYPE,
    CASE
        WHEN CURRENCY_CODE = ''
        OR CURRENCY_CODE IS NULL
        OR regexp_replace(CURRENCY_CODE,
        '[!#$%^&*+=' || chr(34)|| '_{}|<>\;,./?-]',
        '~') LIKE ('%~%')
        OR LENGTH(CURRENCY_CODE)>3 THEN 'error'
        ELSE 'success'
    END AS CURRENCY_CODE_check,
    CURRENCY_CODE,
    CASE
        WHEN ALLOCATED_AMOUNT <> ''
        AND ALLOCATED_AMOUNT IS NOT NULL
        AND LENGTH( regexp_replace(ALLOCATED_AMOUNT,
        '[^a-z_A-Z ]',
        ''))= 0
        AND LENGTH(regexp_replace(REPLACE(ALLOCATED_AMOUNT, chr(13), ''),
        '[-0-9]',
        '')) <= 1
        AND regexp_replace(ALLOCATED_AMOUNT,
        '[!#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
        '~') NOT LIKE ('%~%')
        AND TRIM(ALLOCATED_AMOUNT) NOT LIKE ('-') THEN 'success'
        ELSE 'error'
    END AS ALLOCATED_AMOUNT_check,
    ALLOCATED_AMOUNT,
    CASE
        WHEN EXPENSE_RATIO <> ''
        AND EXPENSE_RATIO IS NOT NULL
        AND LENGTH( regexp_replace(EXPENSE_RATIO,
        '[^a-z_A-Z ]',
        ''))= 0
        AND LENGTH(regexp_replace(REPLACE(EXPENSE_RATIO, chr(13), ''),
        '[-0-9]',
        '')) <= 1
        AND regexp_replace(EXPENSE_RATIO,
        '[!#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',
        '~') NOT LIKE ('%~%')
        AND TRIM(EXPENSE_RATIO) NOT LIKE ('-') THEN 'success'
        ELSE 'error'
    END AS EXPENSE_RATIO_check,
    EXPENSE_RATIO
FROM
    DWH_EXPENSE_RATIO_PLAN_STAGING);


