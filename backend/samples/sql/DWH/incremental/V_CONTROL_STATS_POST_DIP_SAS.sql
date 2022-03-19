CREATE OR REPLACE
VIEW V_CONTROL_STATS_POST_DIP_SAS (SSD_CF,
ESB_CF,
TRT_STAT_NAME,
TRT_STAT_VALUE,
TRT_SEC_STAT_NAME,
TRT_SEC_STAT_VALUE,
CLOSING_QUARTER_CODE,
CLOSING_DATE) AS (

SELECT
	SSD_CF,
	ESB_CF,
	'TRT_DIP_CF' AS TRT_STAT_NAME,
	COUNT(DISTINCT CONTRACT_NUMBER) AS TRT_STAT_VALUE,
	'TRT_SEC_DIP_CF' AS TRT_SEC_STAT_NAME,
	COUNT(DISTINCT CONTRACT_NUMBER || '_' || SECTION_NUMBER) AS TRT_SEC_STAT_VALUE,
	B.CLOSING_QUARTER_CODE,
	A.CLOSING_DATE
FROM
	BI_<env>.PROJECTION_FACT A
JOIN BI_<env>.TCSM_TRT_CHARACS B ON
	CONTRACT_NUMBER = CTR_NF
GROUP BY
	SSD_CF,
	ESB_CF,
	A.CLOSING_DATE,
	B.CLOSING_QUARTER_CODE
	
UNION

SELECT
	SSD_CF,
	ESB_CF,
	'TRT_DIP_RA' AS TRT_STAT_NAME,
	COUNT(DISTINCT CONTRACT_NUMBER) AS TRT_STAT_VALUE,
	'TRT_SEC_DIP_RA' AS TRT_SEC_STAT_NAME,
	COUNT(DISTINCT CONTRACT_NUMBER || '_' || SECTION_NUMBER) AS TRT_SEC_STAT_VALUE,
	B.CLOSING_QUARTER_CODE,
	A.CLOSING_DATE
FROM
	BI_<env>.RD_FACTOR_PROJECTION A
JOIN BI_<env>.TCSM_TRT_CHARACS B ON
	CONTRACT_NUMBER = CTR_NF
GROUP BY
	SSD_CF,
	ESB_CF,
	A.CLOSING_DATE,
	B.CLOSING_QUARTER_CODE
	
UNION

SELECT
	SSD_CF,
	ESB_CF,
	'TRT_DIP_SAS_CF' AS TRT_STAT_NAME,
	COUNT(DISTINCT CTR_NF) AS TRT_STAT_VALUE,
	'TRT_SEC_DIP_SAS_CF' AS TRT_SEC_STAT_NAME,
	COUNT(DISTINCT CTR_NF || '_' || SEC_NF) AS TRT_SEC_STAT_VALUE,
	B.CLOSING_QUARTER_CODE,
	NULL AS CLOSING_DATE
FROM
	BI_<env>.TCSM_PROJECTED_CASHFLOW_DTM A
JOIN BI_<env>.TCSM_TRT_CHARACS B ON
	A.INSURANCE_CONTRACT_GROUP_ID = B.INSURANCE_CONTRACT_GROUP_ID
GROUP BY
	SSD_CF,
	ESB_CF,
	B.CLOSING_QUARTER_CODE
	
UNION



SELECT
	SSD_CF,
	ESB_CF,
	'TRT_DIP_SAS_RA' AS TRT_STAT_NAME,
	COUNT(DISTINCT CTR_NF) AS TRT_STAT_VALUE,
	'TRT_SEC_DIP_SAS_RA' AS TRT_SEC_STAT_NAME,
	COUNT(DISTINCT CTR_NF || '_' || SEC_NF) AS TRT_SEC_STAT_VALUE,
	B.CLOSING_QUARTER_CODE,
	NULL AS CLOSING_DATE
FROM
	BI_<env>.TCSM_RA_RISK_FACTORS A
JOIN BI_<env>.TCSM_TRT_CHARACS B ON
	A.INSURANCE_CONTRACT_GROUP_ID = B.INSURANCE_CONTRACT_GROUP_ID
GROUP BY
	SSD_CF,
	ESB_CF,
	B.CLOSING_QUARTER_CODE
	
UNION

SELECT
	SSD_CF,
	ESB_CF,
	'TRT_DIP_SAS_CU' AS TRT_STAT_NAME,
	COUNT(DISTINCT CTR_NF) AS TRT_STAT_VALUE,
	'TRT_SEC_DIP_SAS_CU' AS TRT_SEC_STAT_NAME,
	COUNT(DISTINCT CTR_NF || '_' || SEC_NF) AS TRT_SEC_STAT_VALUE,
	B.CLOSING_QUARTER_CODE,
	A.CLOSING_DATE
FROM
	(
	SELECT
		ROW_NUMBER() OVER (
		ORDER BY PROJECTION_DT) AS ROW_ID,
		SCENARIO_ID,
		INSURANCE_CONTRACT_GROUP_ID,
		RISK_DRIVER_NM,
		CEDED_FLG,
		CURRENCY_CD,
		PROJECTION_DT,
		SUM (RISK_DRIVER_AMT) AS RISK_DRIVER_AMT,
		CLOSING_DATE
	FROM
		(
		SELECT
			DISTINCT 1 AS SCENARIO_ID,
			CASE
				WHEN TRT.ACCADMTYP_CT IN (1,
				4) THEN CONTRACT_NUMBER || '_' || SECTION_NUMBER || '_' || UNDERWRITING_YEAR
				WHEN TRT.ACCADMTYP_CT IN (3,
				5) THEN CONTRACT_NUMBER || '_' || SECTION_NUMBER || '_' || UNDERWRITING_YEAR
			END AS INSURANCE_CONTRACT_GROUP_ID,
			'COV_UNITS' AS RISK_DRIVER_NM,
			TRT.RETFLG_CF AS CEDED_FLG,
			CURRENCY_CODE AS CURRENCY_CD,
			PROJECTION_DATE,
			CASE
				WHEN QUARTER(PROJECTION_DATE) = 1 THEN '31/03/' || PROJECTION_YEAR
				WHEN QUARTER(PROJECTION_DATE) = 2 THEN '30/06/' || PROJECTION_YEAR
				WHEN QUARTER(PROJECTION_DATE) = 3 THEN '30/09/' || PROJECTION_YEAR
				WHEN QUARTER(PROJECTION_DATE) = 4 THEN '31/12/' || PROJECTION_YEAR
				ELSE ''
			END PROJECTION_DT,
			AMOUNT AS RISK_DRIVER_AMT,
			CLOSING_DATE
		FROM
			BI_<env>.PROJECTION_FACT CU
		INNER JOIN BI_<env>.TCSM_TRT_CHARACS TRT ON
			(CONTRACT_NUMBER || '_' || SECTION_NUMBER || '_' || UNDERWRITING_YEAR = INSURANCE_CONTRACT_GROUP_ID )
		WHERE
			POSITION_ID = '148'
			AND ACCADMTYP_CT <> 2
			AND VALID_TO = '9999-12-31 00:00:00'
			AND SUPP_DATE = '9999-12-31 00:00:00'
			AND YEAR(PROJECTION_DATE) > 2020
			AND LEVEL_OF_ANALYSIS_ID = 30
			AND PROJECTION_MONTH IN (3,
			6,
			9,
			12)
	UNION
		SELECT
			DISTINCT 1 AS SCENARIO_ID,
			CASE
				WHEN TRT.ACCADMTYP_CT IN (2) THEN CONTRACT_NUMBER || '_' || SECTION_NUMBER || '_' || CU.POLICY_UW_YEAR
			END AS INSURANCE_CONTRACT_GROUP_ID,
			'COV_UNITS' AS RISK_DRIVER_NM,
			TRT.RETFLG_CF AS CEDED_FLG,
			CURRENCY_CODE AS CURRENCY_CD,
			PROJECTION_DATE,
			CASE
				WHEN QUARTER(PROJECTION_DATE) = 1 THEN '31/03/' || PROJECTION_YEAR
				WHEN QUARTER(PROJECTION_DATE) = 2 THEN '30/06/' || PROJECTION_YEAR
				WHEN QUARTER(PROJECTION_DATE) = 3 THEN '30/09/' || PROJECTION_YEAR
				WHEN QUARTER(PROJECTION_DATE) = 4 THEN '31/12/' || PROJECTION_YEAR
				ELSE ''
			END PROJECTION_DT,
			AMOUNT AS RISK_DRIVER_AMT,
			CLOSING_DATE
		FROM
			BI_<env>.PROJECTION_FACT CU
		INNER JOIN BI_<env>.TCSM_TRT_CHARACS TRT ON
			(CONTRACT_NUMBER || '_' || SECTION_NUMBER || '_' || CU.POLICY_UW_YEAR = INSURANCE_CONTRACT_GROUP_ID )
		WHERE
			POSITION_ID = '148'
			AND ACCADMTYP_CT = 2
			AND VALID_TO = '9999-12-31 00:00:00'
			AND SUPP_DATE = '9999-12-31 00:00:00'
			AND YEAR(PROJECTION_DATE) > 2020
			AND LEVEL_OF_ANALYSIS_ID = 30
			AND PROJECTION_MONTH IN (3,
			6,
			9,
			12) )
	GROUP BY
		SCENARIO_ID,
		PROJECTION_DT,
		CURRENCY_CD,
		CEDED_FLG,
		RISK_DRIVER_NM,
		INSURANCE_CONTRACT_GROUP_ID,
		CLOSING_DATE
	ORDER BY
		ROW_ID) A
JOIN BI_<env>.TCSM_TRT_CHARACS B ON
	A.INSURANCE_CONTRACT_GROUP_ID = B.INSURANCE_CONTRACT_GROUP_ID
GROUP BY
	SSD_CF,
	ESB_CF,
	A.CLOSING_DATE,
	B.CLOSING_QUARTER_CODE);