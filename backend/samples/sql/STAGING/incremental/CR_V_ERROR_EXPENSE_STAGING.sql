SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_EXPENSE_STAGING;

CREATE OR REPLACE VIEW V_ERROR_EXPENSE_STAGING AS (
	SELECT
		LINENUMBER,
		CASE
			WHEN
				TRIM(SUBSIDIARY) = ''
				OR SUBSIDIARY IS NULL
			THEN
				'Invalid value for Ledger'
			WHEN
				TRIM(SUBSIDIARY) NOT IN (
					SELECT DISTINCT
						CAST(SSD_CF AS VARCHAR)
					FROM
						BI_<env>.TESB
					WHERE
						CAST(SSD_CF AS VARCHAR) = TRIM(SUBSIDIARY)
						AND CAST(ESB_CF AS VARCHAR) = TRIM(SUBLEDGER) 
				)
			THEN
				'Invalid value for Ledger'
			ELSE
				'success'
		END AS SUBSIDIARY_CHECK,
		SUBSIDIARY,
		CASE
			WHEN
				TRIM(SUBLEDGER) = ''
				OR SUBLEDGER IS NULL
			THEN
				'Invalid value for Ledger'
			WHEN
				TRIM(SUBLEDGER) NOT IN (
					SELECT DISTINCT
						CAST(ESB_CF AS VARCHAR)
					FROM
						BI_<env>.TESB
					WHERE
						CAST(ESB_CF AS VARCHAR) = TRIM(SUBLEDGER)
						AND CAST(SSD_CF AS VARCHAR) = TRIM(SUBSIDIARY)
				)
			THEN
				'Invalid value for Ledger'
			WHEN
				TRIM(SUBLEDGER) NOT IN (
					SELECT DISTINCT
						CAST(ESB_CF AS VARCHAR)
					FROM
						BI_<env>.TESB
					WHERE
						LIFE_CF = 2
				)
			THEN
				'Invalid value for P&C Ledger'
			WHEN
				TRIM(SUBLEDGER) NOT IN (
					SELECT DISTINCT
						CAST(ESB_CF AS VARCHAR)
					FROM
						BI_<env>.TESB
					WHERE
						LIFE_CF = 2
				)
			THEN
				'Invalid value for P&C Ledger'
			ELSE
				'success'
		END AS SUBLEDGER_CHECK, 
		SUBLEDGER,
		CASE
			WHEN
				TRIM(MARKET) = ''
				OR MARKET IS NULL
				OR TRIM(MARKET) NOT IN (
					SELECT
						TRIM(C.SGMT_LS)
					FROM
						BI_<env>.TSEGMENTATION B
						JOIN
							BI_<env>.TSEGMT C
							ON B.SGT_NT = C.SGT_NT
							AND B.SGTVER_NT = C.SGTVER_NT
					WHERE
						B.SGTTYP_NT = 8
						AND B.SGTVER_NT = (
							SELECT
								MAX(SGTVER_NT)
							FROM
								BI_<env>.TSEGMENTATION
							WHERE
								SGTTYP_NT = 8
						)
				)
			THEN
				'Invalid value for Market Unit'
			ELSE
				'success'
		END AS MARKET_CHECK,
		MARKET,
        CASE WHEN TRIM(PORTFOLIO) = '' OR PORTFOLIO IS NULL  
		or TRIM(PORTFOLIO) NOT IN (
					SELECT
						TRIM(C.SGMT_LS)
					FROM
						BI_<env>.TSEGMENTATION B
						JOIN
							BI_<env>.TSEGMT C
							ON B.SGT_NT = C.SGT_NT
							AND B.SGTVER_NT = C.SGTVER_NT
					WHERE
						B.SGTTYP_NT = 64
						AND B.SGTVER_NT = (
							SELECT
								MAX(SGTVER_NT)
							FROM BI_<env>.TSEGMENTATION
							WHERE
								SGTTYP_NT = 64
						)
					UNION ALL
					SELECT
						'#'
					FROM
						SYSIBM.SYSDUMMY1
				)
			THEN  
			CASE WHEN PORTFOLIO LIKE 'CRT%' THEN 'success'  else
			'Invalid value for Portfolio' END 
		ELSE   'success'
		END AS PORTFOLIO_CHECK,
		PORTFOLIO,
		CASE
			WHEN
				TRIM(UPPER(NATURE)) NOT IN ('F', 'P', 'N')
			THEN
				'Invalid value for Nature'  
			ELSE
				'success'
		END AS NATURE_CHECK,
		NATURE,
		CASE
			WHEN
				TRIM(CONTRACT_CATEGORY) NOT IN (1, 2, 3)
			THEN
				'Invalid value for Contract Category'     
			ELSE
				'success'
		END AS CONTRACT_CATEGORY_CHECK,
		CONTRACT_CATEGORY,
		CASE
			WHEN
				TRIM(UPPER(RATIO_TYPE)) NOT IN ('A', 'M', '#')
			THEN
				'Invalid value for Ratio Type'
			ELSE
				'success'
		END AS RATIO_TYPE_CHECK,
		RATIO_TYPE,
		CASE
			WHEN
				TRIM(UPPER(SEGMENT_LLOYDS)) NOT IN ('O', 'L')
			THEN
				'Invalid value for Segment Lloyds'  
			ELSE
				'success'
		END AS SEGMENT_LLOYDS_CHECK,
		SEGMENT_LLOYDS, 
		CASE
			WHEN
				TRIM(UPPER(CURRENCY)) <> 'EUR'
			THEN
				'Only EUR currency is expected'
			ELSE
				'success'
		END AS CURRENCY_CHECK,
		CURRENCY
	FROM
		STAGING_EXPENSE_RATIO
);