SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_BASR_TFULLGLT_MA_STG;

CREATE OR REPLACE VIEW V_ERROR_BASR_TFULLGLT_MA_STG AS (
SELECT
    aa.LINE_NUMBER,
    aa.COL_NUMBER ,
    aa.COL_VALUE ,
    aa.ERROR_DESC ,
    e.ERROR_MESSAGE_ID
FROM
    (
    SELECT
        st.LINE_NUMBER ,
        2 AS COL_NUMBER ,
        st.CSUOE AS COL_VALUE,
        CASE
            WHEN LENGTH(st.CSUOE)>21
            OR (LENGTH(st.CSUOE)<20
            AND LENGTH(TRIM(st.CSUOE))>0 )
            OR regexp_replace(st.CSUOE,  '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,?]',  '~') LIKE ('%~%') THEN
            CASE
                WHEN regexp_replace(st.CSUOE, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,.?-]',  '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.CSUOE)>21
                OR (LENGTH(st.CSUOE)<20
                AND LENGTH(TRIM(st.CSUOE))>0 )THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        3 AS COL_NUMBER ,
        st.RCSU AS COL_VALUE,
        CASE
            WHEN LENGTH(st.RCSU)>17     OR (LENGTH(st.RCSU)<16  AND LENGTH(TRIM(st.RCSU))>0)
            OR regexp_replace(st.RCSU, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,?]', '~') LIKE ('%~%') THEN
            CASE
                WHEN regexp_replace(st.RCSU, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,.?-]',  '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.RCSU)>17     OR (LENGTH(st.RCSU)<16   AND LENGTH(TRIM(st.RCSU))>0) THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        4 AS COL_NUMBER ,
        st.BALANCE_SHEET_DATE AS COL_VALUE,
        CASE
            WHEN st.BALANCE_SHEET_DATE = ''
            OR st.BALANCE_SHEET_DATE IS NULL THEN '002 - Mandatory data not mentioned'
            WHEN regexp_replace(st.BALANCE_SHEET_DATE,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]', '~') LIKE ('%~%')
            OR SUBSTRING(st.BALANCE_SHEET_DATE, 1, 1) NOT IN (1, 2,3,  4)
            OR SUBSTRING(st.BALANCE_SHEET_DATE, 2, 1)!= 'Q'
            OR LENGTH(regexp_replace(SUBSTRING(st.BALANCE_SHEET_DATE, 3, 4), '[-0-9]','')) > 0 THEN '005 - Data format is not valid'
            WHEN LENGTH(st.BALANCE_SHEET_DATE)!= 6 THEN '006 - Data length is not valid'
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        5 AS COL_NUMBER ,
        st.RETROCESSIONAIRE AS COL_VALUE,
        CASE
            WHEN LENGTH(st.RETROCESSIONAIRE)>5
            OR regexp_replace(st.RETROCESSIONAIRE, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]','~') LIKE ('%~%') THEN
            CASE
                WHEN regexp_replace(st.RETROCESSIONAIRE,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.RETROCESSIONAIRE)>5 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        6 AS COL_NUMBER ,
        st.TRANSACTION_CODE AS COL_VALUE,
        CASE
            WHEN st.TRANSACTION_CODE = ''
            OR st.TRANSACTION_CODE IS NULL
            OR LENGTH(st.TRANSACTION_CODE)<> 8
            OR regexp_replace(st.TRANSACTION_CODE, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]', '~') LIKE ('%~%') THEN
            CASE
                WHEN st.TRANSACTION_CODE = ''
                OR st.TRANSACTION_CODE IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN regexp_replace(st.TRANSACTION_CODE,  '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.TRANSACTION_CODE)<> 8 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        7 AS COL_NUMBER ,
        st.AMOUNT AS COL_VALUE,
        CASE
            WHEN LENGTH(regexp_replace(st.AMOUNT, '[-0-9]', '')) > 1
            OR LENGTH( regexp_replace(st.AMOUNT,  '[^a-z_A-Z ]', ''))>0
            OR regexp_replace(st.AMOUNT,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]', '~') LIKE ('%~%')
            OR LENGTH(st.AMOUNT)= 0
            OR st.AMOUNT IS NULL
            OR TRIM(st.AMOUNT) LIKE ('-')
            OR (LOCATE('.', st.AMOUNT)!= 0
            AND LENGTH(SUBSTRING(st.amount, LOCATE('.', st.AMOUNT)+ 1) )>3) THEN
            CASE
                WHEN st.AMOUNT = ''
                OR st.AMOUNT IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LOCATE('.', st.AMOUNT)!= 0 AND LENGTH(SUBSTRING(st.amount, LOCATE('.', st.AMOUNT)+ 1) )>3 THEN '005 - Data format is not valid'
                WHEN REGEXP_LIKE(TRIM(st.AMOUNT) , '^\d+(\.\d*)?$') != 'true' THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        8 AS COL_NUMBER ,
        st.USER_COMMENT AS COL_VALUE,
        CASE
            WHEN st.USER_COMMENT = ''
            OR st.USER_COMMENT IS NULL
            OR LENGTH(st.USER_COMMENT)>255 THEN
            CASE
                WHEN st.USER_COMMENT = ''
                OR st.USER_COMMENT IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN LENGTH(st.USER_COMMENT)>255 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        9 AS COL_NUMBER ,
        st.DATA_TYPE_MA AS COL_VALUE,
        CASE
            WHEN st.DATA_TYPE_MA = ''
            OR st.DATA_TYPE_MA IS NULL
            OR LENGTH(st.DATA_TYPE_MA)>20
            OR regexp_replace(st.DATA_TYPE_MA,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]','~') LIKE ('%~%') THEN
            CASE
                WHEN st.DATA_TYPE_MA = ''
                OR st.DATA_TYPE_MA IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN regexp_replace(st.DATA_TYPE_MA, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.DATA_TYPE_MA)>20 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        10 AS COL_NUMBER ,
        st.TEMPORALITY AS COL_VALUE,
        CASE
            WHEN st.TEMPORALITY = ''
            OR st.TEMPORALITY IS NULL
            OR LENGTH(st.TEMPORALITY)>9
            OR regexp_replace(st.TEMPORALITY,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?]','~') LIKE ('%~%') THEN
            CASE
                WHEN st.TEMPORALITY = ''
                OR st.TEMPORALITY IS NULL THEN '002 - Mandatory data not mentioned'
                WHEN regexp_replace(st.TEMPORALITY,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(st.TEMPORALITY)>9 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        11 AS COL_NUMBER ,
        st.END_VALID_DATE AS COL_VALUE,
        CASE
            WHEN LENGTH( regexp_replace(st.end_valid_date,'[^a-z_A-Z ]', ''))>0
            OR regexp_replace(st.END_VALID_DATE,'[-!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,?]','~') LIKE ('%~%')
            OR LENGTH(st.END_VALID_DATE)<10
            OR TRIM(st.END_VALID_DATE) LIKE ('-')
            OR tchk.IS_DATE = 'false' THEN
            CASE
                WHEN LENGTH(st.END_VALID_DATE)!= 10
                AND LENGTH(st.END_VALID_DATE)!= 0 THEN '006 - Data length is not valid'
                WHEN tchk.IS_DATE = 'false'
                AND LENGTH(st.END_VALID_DATE)!= 0 THEN '005 - Data format is not valid'
                WHEN REGEXP_LIKE(TRIM(st.END_VALID_DATE) , '^\d+(\.\d*)?$') != 'true' AND LENGTH(st.END_VALID_DATE)!= 0 THEN '004 - Data type is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
    JOIN WRK_STAGING_BASR_TFULLGLT_MA_DRAFT_T_CHK tchk ON
        st.LINE_NUMBER = tchk.LINE_NUMBER
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        12 AS COL_NUMBER ,
        st.CURRENCY_CODE AS COL_VALUE,
        CASE
            WHEN st.CURRENCY_CODE = ''
            OR st.CURRENCY_CODE IS NULL
            OR LENGTH(REPLACE(st.CURRENCY_CODE, chr(13), ''))!= 3
            OR regexp_replace(st.CURRENCY_CODE, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]', '~') LIKE ('%~%')
            OR LENGTH(REPLACE(st.CURRENCY_CODE, chr(13), ''))= 0 THEN
            CASE
                WHEN st.CURRENCY_CODE = ''
                OR st.CURRENCY_CODE IS NULL
                OR LENGTH(REPLACE(st.CURRENCY_CODE, chr(13), ''))= 0 THEN '002 - Mandatory data not mentioned'
                WHEN regexp_replace(st.CURRENCY_CODE,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]', '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(REPLACE(st.CURRENCY_CODE, chr(13), ''))!= 3 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL

    SELECT
        st.LINE_NUMBER ,
        13 AS COL_NUMBER ,
        st.CLAIMS_ID AS COL_VALUE,
        CASE
            WHEN LENGTH(REPLACE(st.CLAIMS_ID, chr(13), ''))> 8
            OR regexp_replace(st.CLAIMS_ID,   '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',  '~') LIKE ('%~%')
            OR LENGTH(REPLACE(st.CLAIMS_ID, chr(13), ''))= 0 THEN
            CASE
                WHEN regexp_replace(st.CLAIMS_ID, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]',  '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(REPLACE(st.CLAIMS_ID, chr(13), ''))> 8 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        14 AS COL_NUMBER ,
        st.EVENT_ID AS COL_VALUE,
        CASE
            WHEN LENGTH(REPLACE(st.EVENT_ID, chr(13), ''))> 10
            OR regexp_replace(st.EVENT_ID,  '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]', '~') LIKE ('%~%')
            OR LENGTH(REPLACE(st.EVENT_ID, chr(13), ''))= 0 THEN
            CASE
                WHEN regexp_replace(st.EVENT_ID,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(REPLACE(st.EVENT_ID, chr(13), ''))> 10 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st
UNION ALL
    SELECT
        st.LINE_NUMBER ,
        15 AS COL_NUMBER ,
        st.UNIVERSE AS COL_VALUE,
        CASE
            WHEN  st.UNIVERSE = ''
            OR st.UNIVERSE IS NULL 
            OR LENGTH(REPLACE(st.UNIVERSE, chr(13), ''))> 50
            OR regexp_replace(st.UNIVERSE, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]',  '~') LIKE ('%~%')
            OR LENGTH(REPLACE(st.UNIVERSE, chr(13), ''))= 0 THEN
            CASE
                WHEN st.UNIVERSE = ''
            	OR st.UNIVERSE IS NULL THEN '002 - Mandatory data not mentioned'
            	WHEN   regexp_replace(st.UNIVERSE, '[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]', '~') LIKE ('%~%') THEN '004 - Data type is not valid'
                WHEN LENGTH(REPLACE(st.UNIVERSE, chr(13), ''))> 50 THEN '006 - Data length is not valid'
                ELSE '-1'
            END
            ELSE 'success'
        END AS ERROR_DESC
    FROM
       WRK_STAGING_BASR_TFULLGLT_MA_DRAFT st )aa
JOIN DELIVERY_<env>.ERROR_MESSAGE e ON
    e.ERROR_MESSAGE_LABEL = aa.ERROR_DESC );



