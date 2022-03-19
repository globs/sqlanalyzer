SET SCHEMA STAGING_<env>;

DROP VIEW V_COL_ERROR_CHECK_EXPENSE_STAGING;

CREATE OR REPLACE VIEW V_COL_ERROR_CHECK_EXPENSE_STAGING AS (
	SELECT
		aa.LINENUMBER AS LINE_NUMBER,
		aa.col_number,
		aa.col_value,
		aa.error_desc,
		e.error_message_id
	FROM
		(
			SELECT
				st1.LINENUMBER,
				st1.SUBSIDIARY_CHECK,
				4 AS COL_NUMBER,
				st1.SUBSIDIARY AS col_value,   
				CASE
					WHEN
						TRIM(st1.SUBSIDIARY) = ''
						OR st1.SUBSIDIARY IS NULL
					THEN
						'002 - Mandatory data not mentioned'                    
					WHEN
						LENGTH(st1.SUBSIDIARY) > 2
					THEN
						'006 - Data length is not valid'       
					WHEN
						LENGTH(regexp_replace(st1.SUBSIDIARY, '[-0-9]', '')) > 0    
						OR TRIM(st1.SUBSIDIARY) LIKE ('-')
					THEN
						'004 - Data type is not valid'
					ELSE
						st1.SUBSIDIARY_CHECK
				END AS error_desc
			FROM
				V_ERROR_EXPENSE_STAGING st1
			WHERE
				st1.SUBSIDIARY_CHECK <> 'success'
			UNION
			SELECT
				st1.LINENUMBER,
				st1.SUBLEDGER_CHECK,
				5 AS COL_NUMBER,
				st1.SUBLEDGER AS col_value, 
				CASE
					WHEN
						TRIM(st1.SUBLEDGER) = ''
						OR st1.SUBLEDGER IS NULL
					THEN
						'002 - Mandatory data not mentioned'                    
					WHEN
						LENGTH(st1.SUBLEDGER) > 2
					THEN
						'006 - Data length is not valid'
					WHEN
						LENGTH(regexp_replace(st1.SUBLEDGER, '[-0-9]', '')) > 0 
						OR TRIM(st1.SUBLEDGER) LIKE ('-')
					THEN
						'004 - Data type is not valid'      
					ELSE
						st1.SUBLEDGER_CHECK
				END AS error_desc     
			FROM
				V_ERROR_EXPENSE_STAGING st1
			WHERE         
				st1.SUBLEDGER_CHECK <> 'success'
			UNION
			SELECT
				st1.LINENUMBER,
				st1.MARKET_CHECK,
				6 AS COL_NUMBER,
				st1.MARKET AS col_value,     
				CASE
					WHEN
						TRIM(st1.MARKET) = ''
						OR st1.MARKET IS NULL
					THEN
						'002 - Mandatory data not mentioned'                  
					WHEN
						LENGTH(TRIM(st1.MARKET)) > 5
					THEN
						'006 - Data length is not valid'
					WHEN
						regexp_replace(MARKET, '[!@#$%^&*+=' || chr(34) || ':{}|<>\;,./?-]', '~') LIKE ('%~%')
					THEN
						'004 - Data type is not valid'
					ELSE
						st1.MARKET_CHECK
				END AS error_desc
			FROM
				V_ERROR_EXPENSE_STAGING st1  
			WHERE
				st1.MARKET_CHECK <> 'success'
			UNION
			SELECT
				st1.LINENUMBER,  
				st1.PORTFOLIO_CHECK,     
				7 AS COL_NUMBER,
  				st1.PORTFOLIO AS col_value,
  				CASE
  					WHEN
  						TRIM(st1.PORTFOLIO) = ''
  						OR st1.PORTFOLIO IS NULL
					THEN
						'002 - Mandatory data not mentioned'                 
					WHEN
						LENGTH(TRIM(st1.PORTFOLIO)) > 5
					THEN
						'006 - Data length is not valid'        
					WHEN 
						LENGTH(REGEXP_REPLACE(TRIM(st1.PORTFOLIO), '[\d|#]', '')) > 0
					THEN
						'004 - Data type is not valid'       
					ELSE
						st1.PORTFOLIO_CHECK
				END AS error_desc
			FROM
				V_ERROR_EXPENSE_STAGING st1
			WHERE
				st1.PORTFOLIO_CHECK <> 'success'
			UNION
			SELECT
				st1.LINENUMBER,
				st1.NATURE_CHECK,
				8 AS COL_NUMBER,
				st1.NATURE AS col_value,
				CASE
					WHEN
						TRIM(st1.NATURE) = ''
						OR st1.NATURE IS NULL
					THEN
						'002 - Mandatory data not mentioned'
					WHEN
						LENGTH(st1.NATURE) > 1
					THEN
						'006 - Data length is not valid'
					WHEN
						regexp_replace(NATURE, '[!@#$%^&*+=' || chr(34) || ':{}|<>\;,./?-]', '~') LIKE ('%~%')
					THEN
						'004 - Data type is not valid'
					ELSE
						st1.NATURE_CHECK
				END AS error_desc
			FROM
				V_ERROR_EXPENSE_STAGING st1    
			WHERE
				st1.NATURE_CHECK <> 'success'
			UNION
			SELECT
				st1.LINENUMBER,
				st1.CONTRACT_CATEGORY_CHECK,     
				1 AS COL_NUMBER,
  				st1.CONTRACT_CATEGORY AS col_value,
  				CASE
  					WHEN
  						TRIM(st1.CONTRACT_CATEGORY) = ''
  						OR st1.CONTRACT_CATEGORY IS NULL
					THEN
						'002 - Mandatory data not mentioned'      
					WHEN
						LENGTH(st1.CONTRACT_CATEGORY) > 1
					THEN
						'006 - Data length is not valid'       
					WHEN
						LENGTH(regexp_replace(st1.CONTRACT_CATEGORY, '[-0-9]', '')) > 0   
						OR TRIM(st1.CONTRACT_CATEGORY) LIKE ('-')
					THEN
						'004 - Data type is not valid'
					ELSE
						st1.CONTRACT_CATEGORY_CHECK
				END AS error_desc
			FROM
				V_ERROR_EXPENSE_STAGING st1
			WHERE
				st1.CONTRACT_CATEGORY_CHECK <> 'success'
			UNION
  			SELECT
  				st1.LINENUMBER, 
  				st1.RATIO_TYPE_CHECK,    
  				2 AS COL_NUMBER, 
  				st1.RATIO_TYPE AS col_value,
  				CASE
  					WHEN
						TRIM(st1.RATIO_TYPE) = ''
						OR st1.RATIO_TYPE IS NULL
					THEN
						'002 - Mandatory data not mentioned'
  					WHEN
  						regexp_replace(RATIO_TYPE, '[!@#$%^&*+=' || chr(34)|| ':{}|<>\;,./?-]', '~') LIKE ('%~%')
					THEN
						'004 - Data type is not valid'                    
					WHEN
						LENGTH(st1.RATIO_TYPE)>1 THEN '006 - Data length is not valid'      
					ELSE
						st1.RATIO_TYPE_CHECK
				END AS error_desc
			FROM
				V_ERROR_EXPENSE_STAGING st1
			WHERE
				st1.RATIO_TYPE_CHECK <> 'success'
			UNION
			SELECT
				st1.LINENUMBER,
				st1.SEGMENT_LLOYDS_CHECK,
				3 AS COL_NUMBER,
				st1.SEGMENT_LLOYDS AS col_value,
				CASE
					WHEN
						TRIM(st1.SEGMENT_LLOYDS) = ''
						OR st1.SEGMENT_LLOYDS IS NULL
					THEN
						'002 - Mandatory data not mentioned'
					WHEN
						regexp_replace(SEGMENT_LLOYDS, '[!@#$%^&*+=' || chr(34) || ':{}|<>\;.,/?-]', '~') LIKE ('%~%')
					THEN
						'004 - Data type is not valid'                         
					WHEN
						LENGTH(st1.SEGMENT_LLOYDS) > 1
					THEN
						'006 - Data length is not valid'     
					ELSE
						st1.SEGMENT_LLOYDS_CHECK
				END AS error_desc
			FROM
				V_ERROR_EXPENSE_STAGING st1
			WHERE
				st1.SEGMENT_LLOYDS_CHECK <> 'success'
			UNION
			SELECT
				st1.LINENUMBER,
				st1.CURRENCY_CHECK,
				9 AS COL_NUMBER,
				st1.CURRENCY AS col_value,
				CASE
					WHEN
						TRIM(st1.CURRENCY) = ''
						OR st1.CURRENCY IS NULL
					THEN
						'002 - Mandatory data not mentioned'
					WHEN
						regexp_replace(CURRENCY, '[!@#$%^&*+=' || chr(34) || ':{}|<>\;,./?-]', '~') LIKE ('%~%')
					THEN
						'004 - Data type is not valid'
					WHEN
						LENGTH(st1.CURRENCY) != 3
					THEN
						'006 - Data length is not valid'
					ELSE
						st1.CURRENCY_CHECK
				END AS error_desc
			FROM
				V_ERROR_EXPENSE_STAGING st1
			WHERE
				st1.CURRENCY_CHECK <> 'success'
		) aa
		LEFT JOIN
			DELIVERY_<env>."ERROR_MESSAGE" e
			ON e.ERROR_MESSAGE_LABEL = aa.error_desc
);