SET SCHEMA STAGING_<env>;

DROP VIEW V_ERROR_CHECK_ILIAS_TO_DIP;

CREATE OR REPLACE VIEW V_ERROR_CHECK_ILIAS_TO_DIP AS
( 
	SELECT     
		aa.LINE_NUMBER,     
		aa.col_number,     
		aa.col_value,     
		aa.error_desc,     
		e.error_message_id 
	FROM    
	(     
		SELECT         
			st.LINE_NUMBER,         
			2 AS COL_NUMBER,         
			st.SUBSIDIARY_CODE AS col_value,         
			CASE WHEN LENGTH(regexp_replace(st.SUBSIDIARY_CODE,'[-0-9]','')) > 0             
					 OR TRIM(st.SUBSIDIARY_CODE) LIKE ('-') THEN             
				CASE WHEN LENGTH(regexp_replace(st.SUBSIDIARY_CODE,'[-0-9]','')) > 0                 
					 OR TRIM(st.SUBSIDIARY_CODE) LIKE ('-') THEN '004 - Data type is not valid'                 
				ELSE '-1'             
				END             
			ELSE 'success'         
			END AS error_desc     
		FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
		UNION     
		SELECT         
			st.LINE_NUMBER,         
			3 AS COL_NUMBER,         
			st.LEDGER_CODE AS col_value,         
			CASE WHEN LENGTH(regexp_replace(st.LEDGER_CODE,'[-0-9]','')) > 0             
					OR TRIM(st.LEDGER_CODE) LIKE ('-') THEN             
				CASE WHEN LENGTH(regexp_replace(st.LEDGER_CODE,'[-0-9]','')) > 0                 
					OR TRIM(st.LEDGER_CODE) LIKE ('-') THEN '004 - Data type is not valid'                 
				ELSE '-1'             
				END             
			ELSE 'success'         
			END AS error_desc     
		FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
		UNION     
		SELECT         
			st.LINE_NUMBER,         
			4 AS COL_NUMBER,         
			st.REGION_CODE AS col_value,         
			CASE WHEN LENGTH(st.REGION_CODE)>12 THEN             
				CASE WHEN LENGTH(st.REGION_CODE)>12 THEN '006 - Data length is not valid'                 
				ELSE '-1'             
				END             
			ELSE 'success'         
			END AS error_desc     
			FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					5 AS COL_NUMBER,         
					st.TREATY_LIFE_CHARACTERISTIC_CODE AS col_value,         
					CASE WHEN LENGTH(st.TREATY_LIFE_CHARACTERISTIC_CODE)>8 THEN             
						CASE WHEN LENGTH(st.TREATY_LIFE_CHARACTERISTIC_CODE)>8 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
					END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					6 AS COL_NUMBER,         
					st.FINANCING_TYPE_CODE AS col_value,         
					CASE WHEN LENGTH(st.FINANCING_TYPE_CODE)>20 THEN             
						CASE WHEN LENGTH(st.FINANCING_TYPE_CODE)>20 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					7 AS COL_NUMBER,         
					st.IAS39_CODE AS col_value,         
					CASE WHEN LENGTH(regexp_replace(st.IAS39_CODE,'[-0-9]','')) > 0             
						OR TRIM(st.IAS39_CODE) LIKE ('-') THEN             
						CASE WHEN LENGTH(regexp_replace(st.IAS39_CODE,'[-0-9]','')) > 0                 
							OR TRIM(st.IAS39_CODE) LIKE ('-') THEN '004 - Data type is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
					FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					8 AS COL_NUMBER,         
					st.USGAAP_CODE AS col_value,         
					CASE WHEN LENGTH(regexp_replace(st.USGAAP_CODE,'[-0-9]','')) > 0             
						OR TRIM(st.USGAAP_CODE) LIKE ('-') THEN             
						CASE WHEN LENGTH(regexp_replace(st.USGAAP_CODE,'[-0-9]','')) > 0                 
							OR TRIM(st.USGAAP_CODE) LIKE ('-') THEN '004 - Data type is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					9 AS COL_NUMBER,         
					st.COINSURANCE_CODE AS col_value,         
					CASE WHEN LENGTH(st.COINSURANCE_CODE)>8 THEN             
						CASE WHEN LENGTH(st.COINSURANCE_CODE)>8 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT 
					st.LINE_NUMBER,         
					10 AS COL_NUMBER,         
					st.TYPE_OF_BUSINESS_CODE AS col_value,         
					CASE WHEN LENGTH(regexp_replace(st.TYPE_OF_BUSINESS_CODE,'[-0-9]','')) > 0             
						OR TRIM(st.TYPE_OF_BUSINESS_CODE) LIKE ('-') THEN             
						CASE WHEN LENGTH(regexp_replace(st.TYPE_OF_BUSINESS_CODE,'[-0-9]','')) > 0                 
							OR TRIM(st.TYPE_OF_BUSINESS_CODE) LIKE ('-') THEN '004 - Data type is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					11 AS COL_NUMBER,         
					st.OMEGA_LOB_CODE AS col_value,         
					CASE WHEN st.OMEGA_LOB_CODE = '' 
						OR st.OMEGA_LOB_CODE IS NULL 
						OR LENGTH(st.OMEGA_LOB_CODE)>8 THEN             
						CASE WHEN st.OMEGA_LOB_CODE = '' 
							OR st.OMEGA_LOB_CODE IS NULL THEN '002 - Mandatory data not mentioned'                 
							WHEN LENGTH(st.OMEGA_LOB_CODE)>8 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
					FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					12 AS COL_NUMBER,         
					st.GUARANTEE_CODE AS col_value,         
					CASE WHEN st.GUARANTEE_CODE = ''             
						OR st.GUARANTEE_CODE IS NULL             
						OR LENGTH(st.GUARANTEE_CODE)>12 THEN             
						CASE WHEN st.GUARANTEE_CODE = ''                 
							OR st.GUARANTEE_CODE IS NULL THEN '002 - Mandatory data not mentioned'                 
							WHEN LENGTH(st.GUARANTEE_CODE)>12 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
					FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					13 AS COL_NUMBER,         
					st.POLICY_TYPE_CODE AS col_value,         
					CASE WHEN LENGTH(st.POLICY_TYPE_CODE)>12 THEN             
						CASE WHEN LENGTH(st.POLICY_TYPE_CODE)>12 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					14 AS COL_NUMBER,         
					st.NATURE_CODE AS col_value,         
					CASE WHEN LENGTH(st.NATURE_CODE)>16 THEN             
						CASE WHEN LENGTH(st.NATURE_CODE)>16 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					15 AS COL_NUMBER,         
					st.BASIS_TYPE AS col_value,         
					CASE WHEN LENGTH(st.BASIS_TYPE)>3 THEN             
						CASE WHEN LENGTH(st.BASIS_TYPE)>3 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					16 AS COL_NUMBER,         
					st.COUNTER_PARTY_ID AS col_value,         
					CASE WHEN LENGTH(st.COUNTER_PARTY_ID)>40 THEN             
						CASE WHEN LENGTH(st.COUNTER_PARTY_ID)>40 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					17 AS COL_NUMBER,         
					st.ACTUARIALMODELTYPE_CODE AS col_value,         
					CASE WHEN LENGTH(regexp_replace(st.ACTUARIALMODELTYPE_CODE,'[-0-9]','')) > 0             
						OR TRIM(st.ACTUARIALMODELTYPE_CODE) LIKE ('-') THEN             
						CASE WHEN LENGTH(regexp_replace(st.ACTUARIALMODELTYPE_CODE,'[-0-9]','')) > 0                 
							OR TRIM(st.ACTUARIALMODELTYPE_CODE) LIKE ('-') THEN '004 - Data type is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT 
					st.LINE_NUMBER,         
					18 AS COL_NUMBER,         
					st.CLIENT_ID AS col_value,         
					CASE WHEN LENGTH(st.CLIENT_ID)>40 THEN             
						CASE WHEN LENGTH(st.CLIENT_ID)>40 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					19 AS COL_NUMBER,         
					st.TERMTYPE_CODE AS col_value,         
					CASE WHEN LENGTH(regexp_replace(st.TERMTYPE_CODE,'[-0-9]','')) > 0             
						OR TRIM(st.TERMTYPE_CODE) LIKE ('-') THEN             
						CASE WHEN LENGTH(regexp_replace(st.TERMTYPE_CODE,'[-0-9]','')) > 0                 
							OR TRIM(st.TERMTYPE_CODE) LIKE ('-') THEN '004 - Data type is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					20 AS COL_NUMBER,         
					st.BUSINESSMATURITY_CODE AS col_value,         
					CASE WHEN LENGTH(st.BUSINESSMATURITY_CODE)>16 THEN             
						CASE WHEN LENGTH(st.BUSINESSMATURITY_CODE)>16 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					21 AS COL_NUMBER,         
					st.BUSINESSMATURITY_FLAG AS col_value,         
					CASE WHEN LENGTH(st.BUSINESSMATURITY_FLAG)>1 THEN             
						CASE WHEN LENGTH(st.BUSINESSMATURITY_FLAG)>1 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					22 AS COL_NUMBER,         
					st.MARKET_UNIT_CODE AS col_value,         
					CASE WHEN LENGTH(regexp_replace(st.MARKET_UNIT_CODE,'[-0-9]','')) > 0             
						OR TRIM(st.MARKET_UNIT_CODE) LIKE ('-') THEN             
						CASE WHEN LENGTH(regexp_replace(st.MARKET_UNIT_CODE,'[-0-9]','')) > 0                 
							OR TRIM(st.MARKET_UNIT_CODE) LIKE ('-') THEN '004 - Data type is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					23 AS COL_NUMBER,         
					st.LEVELOFANALYSIS_CODE AS col_value,         
					CASE WHEN st.LEVELOFANALYSIS_CODE = ''             
						OR st.LEVELOFANALYSIS_CODE IS NULL             
						OR LENGTH(st.LEVELOFANALYSIS_CODE)>32 THEN             
						CASE WHEN st.LEVELOFANALYSIS_CODE = ''                 
							OR st.LEVELOFANALYSIS_CODE IS NULL THEN '002 - Mandatory data not mentioned'                 
							WHEN LENGTH(st.LEVELOFANALYSIS_CODE)>32 THEN '006 - Data length is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					24 AS COL_NUMBER,         
					st.CSM_CASHFLOW_LEGS_CODE AS col_value,         
					CASE WHEN st.CSM_CASHFLOW_LEGS_CODE = ''             
						OR st.CSM_CASHFLOW_LEGS_CODE IS NULL             
						OR LENGTH(st.CSM_CASHFLOW_LEGS_CODE)>32 THEN             
						CASE WHEN st.CSM_CASHFLOW_LEGS_CODE = ''                 
							OR st.CSM_CASHFLOW_LEGS_CODE IS NULL THEN '002 - Mandatory data not mentioned'                 
							WHEN LENGTH(st.CSM_CASHFLOW_LEGS_CODE)>32 THEN '006 - Data length is not valid'                 
							ELSE '-1'             
							END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT         
					st.LINE_NUMBER,         
					25 AS COL_NUMBER,         
					st.PV_FLAG AS col_value,         
					CASE WHEN st.PV_FLAG = ''             
						OR st.PV_FLAG IS NULL             
						OR UPPER(st.PV_FLAG)!= 'Y'             
						OR UPPER(st.PV_FLAG)!= 'N' THEN             
						CASE WHEN st.PV_FLAG = ''                 
							OR st.PV_FLAG IS NULL THEN '002 - Mandatory data not mentioned'                 
							WHEN UPPER(st.PV_FLAG)!= 'Y'                 
								AND UPPER(st.PV_FLAG)!= 'N' THEN '004 - Data type is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT 
					st.LINE_NUMBER,         
					26 AS COL_NUMBER,         
					st.PERIOD_ID AS col_value,         
					CASE WHEN st.PERIOD_ID = ''             
						OR st.PERIOD_ID IS NULL             
						OR LENGTH(regexp_replace(st.PERIOD_ID,'[-0-9]','')) > 0             
						OR TRIM(st.PERIOD_ID) LIKE ('-') THEN             
						CASE WHEN st.PERIOD_ID = ''                 
							OR st.PERIOD_ID IS NULL THEN '002 - Mandatory data not mentioned'                 
							WHEN LENGTH(regexp_replace(st.PERIOD_ID,'[-0-9]','')) > 0                 
								OR TRIM(st.PERIOD_ID) LIKE ('-') THEN '004 - Data type is not valid'                 
						ELSE '-1'             
						END             
					ELSE 'success'         
					END AS error_desc     
				FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
			UNION     
				SELECT 
					st.LINE_NUMBER,         
					27 AS COL_NUMBER,         
					st.RA_PERIOD AS col_value,         
					CASE WHEN st.RA_PERIOD = ''             
						OR st.RA_PERIOD IS NULL             
						OR LENGTH(regexp_replace(st.RA_PERIOD,'[-0-9]','')) > 0             
						OR TRIM(st.RA_PERIOD) LIKE ('-') THEN             
						CASE WHEN st.RA_PERIOD = ''                 
							OR st.RA_PERIOD IS NULL THEN '002 - Mandatory data not mentioned'                 
							WHEN LENGTH(regexp_replace(st.RA_PERIOD,'[-0-9]','')) > 0                 
								OR TRIM(st.RA_PERIOD) LIKE ('-') THEN '004 - Data type is not valid'                 
							ELSE '-1'             
							END             
						ELSE 'success'         
						END AS error_desc     
					FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
				UNION     
					SELECT         
						st.LINE_NUMBER,         
						28 AS COL_NUMBER,         
						st.RA_YEAR AS col_value,         
						CASE WHEN st.RA_YEAR = ''             
							OR st.RA_YEAR IS NULL             
							OR LENGTH(regexp_replace(st.RA_YEAR,'[-0-9]','')) > 0             
							OR TRIM(st.RA_YEAR) LIKE ('-') THEN             
							CASE WHEN st.RA_YEAR = ''                 
								OR st.RA_YEAR IS NULL THEN '002 - Mandatory data not mentioned'                 
								WHEN LENGTH(regexp_replace(st.RA_YEAR,'[-0-9]','')) > 0                 
									OR TRIM(st.RA_YEAR) LIKE ('-') THEN '004 - Data type is not valid'                 
							ELSE '-1'             
							END             
						ELSE 'success'         
						END AS error_desc     
					FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
				UNION     
					SELECT         
						st.LINE_NUMBER,         
						29 AS COL_NUMBER,         
						st.RA_FACTOR AS col_value,         
						CASE WHEN LENGTH(regexp_replace(st.RA_FACTOR,'[-0-9]','')) > 1             
							OR LENGTH( regexp_replace(st.RA_FACTOR,'[^a-z_A-Z ]',''))>0             
							OR LENGTH(st.RA_FACTOR)= 0             
							OR st.RA_FACTOR IS NULL             
							OR TRIM(st.RA_FACTOR) LIKE ('-') THEN             
							CASE WHEN st.RA_FACTOR = ''                 
								OR st.RA_FACTOR IS NULL THEN '002 - Mandatory data not mentioned'                 
								WHEN REGEXP_LIKE(TRIM(st.RA_FACTOR) ,'^\d+(\.\d*)?$') != 'true' THEN '004 - Data type is not valid'            
							ELSE '-1'             
							END             
						ELSE 'success'         
						END AS error_desc     
					FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
				UNION     
					SELECT         
						st.LINE_NUMBER,         
						30 AS COL_NUMBER,         
						st.Reporting_Basis_Code AS col_value,         
						CASE WHEN st.Reporting_Basis_Code = ''             
							OR st.Reporting_Basis_Code IS NULL             
							OR LENGTH(st.Reporting_Basis_Code)>16 THEN             
							CASE WHEN st.Reporting_Basis_Code = ''                 
								OR st.Reporting_Basis_Code IS NULL THEN '002 - Mandatory data not mentioned'                 
								WHEN LENGTH(st.Reporting_Basis_Code)>16 THEN '006 - Data length is not valid'                 
							ELSE '-1'             
							END             
						ELSE 'success'         
						END AS error_desc     
					FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
				UNION     
					SELECT         
						st.LINE_NUMBER,         
						31 AS COL_NUMBER,         
						st.CLOSING_DATE AS col_value,         
						CASE WHEN REPLACE(st.closing_date, chr(13), '') = ''             
							OR REPLACE(st.closing_date, chr(13), '') IS NULL THEN '002 - Mandatory data not mentioned'             
							WHEN LENGTH(REPLACE(st.closing_date, chr(13), '')) <> 10 THEN '006 - Data length is not valid'             
							WHEN LENGTH(regexp_replace(REPLACE(st.closing_date, chr(13), ''),'[0-9]','')) != 2             
								OR regexp_replace(REPLACE(st.closing_date, chr(13),''),'[!@#$%^&*+=' || chr(34)|| ':_{}|<>\;,.?]','~') LIKE ('%~%') 
							THEN '004 - Data type is not valid'             
						ELSE 'success'         
						END AS error_desc     
					FROM STAGING_RISK_ADJUSTMENT_FACTOR_PARAM st 
	)aa 
	JOIN DELIVERY_<env>."ERROR_MESSAGE" e 
	ON e.ERROR_MESSAGE_LABEL = aa.error_desc 
);