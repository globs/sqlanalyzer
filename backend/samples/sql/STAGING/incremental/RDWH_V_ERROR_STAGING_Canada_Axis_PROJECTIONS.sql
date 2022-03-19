SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_STAGING_CANADA_AXIS_PROJECTIONS;

CREATE OR REPLACE
VIEW V_ERROR_STAGING_CANADA_AXIS_PROJECTIONS AS (
SELECT
	aa.line_number,
	aa.col_number,
	aa.col_value,
	aa.error_desc,
	e.error_message_id
FROM
	(
	SELECT
		st.LINE_NUMBER,
		1 AS COL_NUMBER,
		st.NAME AS col_value,
		CASE
			WHEN LENGTH(st.NAME) > 100 THEN '006 - Data length is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		2 AS COL_NUMBER,
		st.CURRENT_DATE_TIME AS col_value,
		CASE
			WHEN (st.CURRENT_DATE_TIME) <> ''
			AND LENGTH(st.CURRENT_DATE_TIME) <> 10 THEN '006 - Data length is not valid'
			WHEN (st.CURRENT_DATE_TIME) <> ''
			AND LENGTH(regexp_replace(st.CURRENT_DATE_TIME,'[0-9]','')) != 2
			OR regexp_replace(st.CURRENT_DATE_TIME,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,.?]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		3 AS COL_NUMBER,
		st.DATASET_LONG_NAME AS col_value,
		CASE
			WHEN LENGTH(st.DATASET_LONG_NAME) > 200 THEN '006 - Data length is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		4 AS COL_NUMBER,
		st.REPORT_NAME AS col_value,
		CASE
			WHEN LENGTH(st.REPORT_NAME) > 100 THEN '006 - Data length is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		5 AS COL_NUMBER,
		st.VALUATION_DATE AS col_value,
		CASE
			WHEN st.VALUATION_DATE = ''
			OR st.VALUATION_DATE IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.VALUATION_DATE) <> 10 THEN '006 - Data length is not valid'
			WHEN LENGTH(regexp_replace(st.VALUATION_DATE,'[0-9]','')) != 2
			OR regexp_replace(st.VALUATION_DATE,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,.?]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		6 AS COL_NUMBER,
		st.CLOSING_DATE AS col_value,
		CASE
			WHEN st.CLOSING_DATE = ''
			OR st.CLOSING_DATE IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.CLOSING_DATE) <> 10 THEN '006 - Data length is not valid'
			WHEN LENGTH(regexp_replace(st.CLOSING_DATE,'[0-9]','')) != 2
			OR regexp_replace(st.CLOSING_DATE,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,.?]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		7 AS COL_NUMBER,
		st.POLICY_UWY AS col_value,
		CASE
			WHEN LENGTH(st.POLICY_UWY) > 4 THEN '006 - Data length is not valid'
			WHEN LENGTH(regexp_replace(st.POLICY_UWY,'[-0-9]','')) > 0
			OR TRIM(st.POLICY_UWY) LIKE ('-%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		8 AS COL_NUMBER,
		st.BASIS AS col_value,
		CASE
			WHEN st.BASIS = ''
			OR st.BASIS IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.BASIS) > 100 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.BASIS,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		9 AS COL_NUMBER,
		st.BUSINESS_MATURITY AS col_value,
		CASE
			WHEN st.BUSINESS_MATURITY = ''
			OR st.BUSINESS_MATURITY IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.BUSINESS_MATURITY) > 50 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.BUSINESS_MATURITY,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		10 AS COL_NUMBER,
		st.SENSITIVITY_TYPE AS col_value,
		CASE
			WHEN st.SENSITIVITY_TYPE = ''
			OR st.SENSITIVITY_TYPE IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.SENSITIVITY_TYPE) > 100 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.BUSINESS_MATURITY,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		11 AS COL_NUMBER,
		st.SENSITIVITY_VALUE AS col_value,
		CASE
			WHEN st.SENSITIVITY_VALUE = ''
			OR st.SENSITIVITY_VALUE IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.SENSITIVITY_VALUE) > 10 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.SENSITIVITY_VALUE,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]','~') LIKE ('%~%') and REGEXP_LIKE(TRIM(st.SENSITIVITY_VALUE) ,'^\d+(\.\d*)?$') != 'true' THEN '004 - Data type is not valid' 
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		12 AS COL_NUMBER,
		st.AOC_STEP AS col_value,
		CASE
			WHEN st.AOC_STEP = ''
			OR st.AOC_STEP IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.AOC_STEP) > 100 THEN '006 - Data length is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		13 AS COL_NUMBER,
		st.AOC_STEP_DETAIL AS col_value,
		CASE
			WHEN LENGTH(st.AOC_STEP_DETAIL) > 100 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.AOC_STEP_DETAIL,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		14 AS COL_NUMBER,
		st.GROSS_ASSUMED_OMEGA_TREATY_NUMBER AS col_value,
		CASE
			WHEN LENGTH(st.GROSS_ASSUMED_OMEGA_TREATY_NUMBER) > 9 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.GROSS_ASSUMED_OMEGA_TREATY_NUMBER,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		15 AS COL_NUMBER,
		st.GROSS_ASSUMED_OMEGA_TREATY_SECTION AS col_value,
		CASE
			WHEN LENGTH(st.GROSS_ASSUMED_OMEGA_TREATY_SECTION) > 2 THEN '006 - Data length is not valid'
			WHEN LENGTH(regexp_replace(st.GROSS_ASSUMED_OMEGA_TREATY_SECTION,'[-0-9]','')) > 0
			OR TRIM(st.GROSS_ASSUMED_OMEGA_TREATY_SECTION) LIKE ('-%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		16 AS COL_NUMBER,
		st.OMEGA_TREATY_NUMBER AS col_value,
		CASE
			WHEN st.OMEGA_TREATY_NUMBER = ''
			OR st.OMEGA_TREATY_NUMBER IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.OMEGA_TREATY_NUMBER) <> 9 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.OMEGA_TREATY_NUMBER,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		17 AS COL_NUMBER,
		st.OMEGA_SECTION AS col_value,
		CASE
			WHEN st.OMEGA_SECTION = ''
			OR st.OMEGA_SECTION IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.OMEGA_SECTION) > 2 THEN '006 - Data length is not valid'
			WHEN LENGTH(regexp_replace(st.OMEGA_SECTION,'[-0-9]','')) > 0
			OR TRIM(st.OMEGA_SECTION) LIKE ('-%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		18 AS COL_NUMBER,
		st.PRODUCT_TYPE AS col_value,
		CASE
			WHEN LENGTH(st.PRODUCT_TYPE) > 100 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.PRODUCT_TYPE,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		19 AS COL_NUMBER,
		st.TYPE AS col_value,
		CASE
			WHEN LENGTH(st.TYPE) > 2 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.TYPE,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		20 AS COL_NUMBER,
		st.POSITION AS col_value,
		CASE
			WHEN st.POSITION = ''
			OR st.POSITION IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.POSITION) > 100 THEN '006 - Data length is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		st.LINE_NUMBER,
		21 AS COL_NUMBER,
		st.CURRENCY AS col_value,
		CASE
			WHEN st.CURRENCY = ''
			OR st.CURRENCY IS NULL THEN '002 - Mandatory data not mentioned'
			WHEN LENGTH(st.CURRENCY) > 3 THEN '006 - Data length is not valid'
			WHEN regexp_replace(st.CURRENCY,'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,./?-]','~') LIKE ('%~%') THEN '004 - Data type is not valid'
			ELSE 'success'
		END AS error_desc
	FROM
		STAGING_CAN_AXIS_PROJECTIONS st
UNION
	SELECT
		LINE_NUMBER,
		COL_NUMBER,
		col_value,
		error_desc
	FROM
		(
		SELECT
			st.LINE_NUMBER AS LINE_NUMBER ,
			SYS.COLNO + 1 AS COL_NUMBER ,
			st.PROJECTION_AMOUNT AS col_value,
			PERIOD,
			CASE
				WHEN LENGTH(regexp_replace(REPLACE(st.PROJECTION_AMOUNT, chr(13), ''),'[-0-9]','')) > 1
				OR regexp_replace(REPLACE(st.PROJECTION_AMOUNT, chr(13), ''),'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,/?]','~') LIKE ('%~%')
				OR TRIM(st.PROJECTION_AMOUNT) LIKE ('-') THEN
				CASE
					WHEN regexp_like(TRIM(REPLACE(st.PROJECTION_AMOUNT, chr(13), '')),'^\d+(\.\d*)?$') != 'true' THEN '004 - Data type is not valid'
					ELSE '-1'
				END
				ELSE 'success'
			END AS error_desc
		FROM
			STAGING_CAN_AXIS_PROJECTIONS st
		JOIN SYSCAT.COLUMNS SYS ON
			SYS.COLNAME = 'TEMP_' || PERIOD
			AND TABNAME = 'WRK_CAN_AXIS_PROJECTIONS_DYNAMIC'
			AND TABSCHEMA = 'STAGING_<env>') )aa
JOIN DELIVERY_<env>."ERROR_MESSAGE" e ON
	e.ERROR_MESSAGE_LABEL = aa.error_desc ) ;