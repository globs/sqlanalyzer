SET SCHEMA SHARE_<env>;

CREATE OR REPLACE VIEW V_SECTION AS
	SELECT
		'A' AS Assumed_Retro_FLAG,
		TSECTION_TUWSEC.CTR_NF AS Contract,
		TSECTION_TUWSEC.UWY_NF AS Underwriting_Year,
		TSECTION_TUWSEC.END_NT,
		TSECTION_TUWSEC.UW_NT,
		TSECTION_TUWSEC.SEC_NF,
		TSECTION_TUWSEC.SSD_CF,
        TSECTION_TUWSEC.SUBLEDGER_CF,
		TO_CHAR(TUWCTR.CTRINC_D,'YYYYMMDD') AS Contract_Inception_date,
		TO_CHAR(TUWCTR.CTREXP_D,'YYYYMMDD') AS Contract_Expiry_date,
		TUWCTR.RENTYP_CT AS Duration_type,
		TCONTR_TBANTECL3.COLVAL_LS AS Duration_type_lab,
		TUWCTR.CTRSTS_CT AS Contract_Status,
		TUWCTR.CTRSTS_LS AS Contract_Status_lab,
		TSECTION_TUWSEC.CTRNAT_CT AS Contract_nature,
		TSECTION_TUWSEC.CTRNAT_LS AS Contract_NATURE_LAB,
		TSECTION_TUWSEC.LIFLOBA_CF AS External_LOB,
		TSECTION_TUWSEC.LIFLOBA_LL AS External_LOB_LAB,
		TSECTION_TUWSEC.LIFFINSOLA_CF AS Financial_Solution,
		TSECTION_TUWSEC.LIFFINSOLA_LL AS Financial_Solution_lab,
		TSECTION_TUWSEC.ASSFINANCE_CT AS IFRS,
		TSECTION_TBANTECL5.COLVAL_LS AS IFRS_LAB,
		TSECTION_TUWSEC.LOB_CF AS LOB,
		TSECTION_TLOBL.LOB_GS AS LOB_LAB,
		TO_CHAR(TCONTR.SCOINC_D,'YYYYMMDD') AS Original_Inception_date,
		TSECTION_TUWSEC.LIFPRDLINA_CF AS Product_Line,
		TSECTION_TUWSEC.LIFPRDLINA_LL AS Product_Line_LAB,
		TSECTION_TUWSEC.SECQUA2_CF AS Qualifier_2_Section,
		TSECTION_TBANALL3.COLVAL_LS AS Qualifier_2_Section_LAB,
		TO_CHAR(TSECTION_TUWSEC.SECCAN_D,'YYYYMMDD') AS Section_Cancellation_date,
		TSECTION_TUWSEC.SECSTS_CT AS Section_Status,
		TO_CHAR(TSECTION_TUWSEC.SECINC_D,'YYYYMMDD') AS Section_Inception_date,
		TSECTION_TUWSEC.SOB_CF,
		TSECTION_TSOBL.SOB_GS,
		CAST(TSECTION_TUWSEC.SUBMRK_NT AS VARCHAR(6)) AS Market,
		TSECTION_TUWSEC.SUBMRK_LS AS Market_lab,
		TSECTION_TUWSEC.TOP_CF,
		TSECTION_TTOPL.TOP_GS,
		LTRIM(RTRIM(CAST(TSECTION_TUWSEC.UWGRP_CF AS VARCHAR(5)))) AS Underwriting_unit,
		TSECTION_TUWSEC.ANLCTY_CF AS Analysis_Country,
		TSECTION_TUWSEC.GAR_CF AS Guarantee,
		TSECTION_TGARL.GAR_GS AS Guarantee_LAB,
		TSECTION_TUWSEC.LIFTRTTYP_CF AS Life_treaty_type,
		TSECTION_TUWSEC.LIFTRTTYP_LS AS Life_Treaty_type_lab,
		TSECTION_TUWSEC.NAT_CF AS Nature,
		TSECTION_TCTRNATL.CTRNAT_GS AS Nature_LAB,
		TSECTION_TUWSEC.CED_NF AS Cedent,
		TSECTION_TUWSEC.START_D AS Start_date,
		TSECTION_TUWSEC.END_D AS End_date,
		TSECTION_TUWSEC.USGAAP_CT AS Term_Type,
		TSECTION_TBANTECL6.COLVAL_LM AS Term_Type_description,
		CASE WHEN TSECIFRS.GRPIFRSSEG_CT IS NULL 
			AND TSECIFRS.GRPIFRSSEG1_CT IS NULL 
			AND TSECIFRS.GRPIFRSSEG1_CT IS NULL
		THEN NULL 
		ELSE(COALESCE(TRIM(TSECIFRS.GRPIFRSSEG_CT),'')||'-'||
			COALESCE(TRIM(TSECIFRS.GRPIFRSSEG1_CT),'')||'-'||
			COALESCE(TRIM(TSECIFRS.GRPINIPRO_CF),'')) 
		END AS IFRS17_Cohort, 
		CASE WHEN TSECIFRS.GRPIFRSSEG_LL IS NULL 
			AND TSECIFRS.GRPIFRSSEG1_LL IS NULL 
			AND TSECIFRS_TBANALL2.COLVAL_LS IS NULL 
		THEN NULL 
		ELSE (COALESCE(TSECIFRS.GRPIFRSSEG_LL,'')||'-'||
			COALESCE(TSECIFRS.GRPIFRSSEG1_LL,'')||'-'||
			COALESCE(TSECIFRS_TBANALL2.COLVAL_LS,''))
		END AS IFRS17_Cohort_LAB,
		TSECIFRS.GRPIFRSSEG_CT AS IFRS17_Portfolio,
		TSECIFRS.GRPIFRSSEG_LL AS IFRS17_Portfolio_LAB,
		TSECIFRS.GRPIFRSSEG1_CT AS IFRS17_Sub_Portfolio,
		TSECIFRS.GRPIFRSSEG1_LL AS IFRS17_Sub_Portfolio_LAB,
		TSECIFRS.GRPIFRSTRA_CT AS Transition_Mode,
		TSECIFRS_TBANALL1.COLVAL_LS AS Transition_Mode_LAB,
		TSECIFRS.GRPINIPRO_CF AS Initial_Profitability,
		TSECIFRS_TBANALL2.COLVAL_LS AS Initial_Profitability_LAB
	FROM
			BI_<env>.TUWSEC TSECTION_TUWSEC
	LEFT OUTER JOIN BI_<env>.TSECTION TSECTION ON
			TSECTION_TUWSEC.CTR_NF = TSECTION.CTR_NF
			AND TSECTION_TUWSEC.SEC_NF = TSECTION.SEC_NF
			AND TSECTION_TUWSEC.END_NT = TSECTION.END_NT
			AND TSECTION_TUWSEC.UWY_NF = TSECTION.UWY_NF
			AND TSECTION_TUWSEC.UW_NT = TSECTION.UW_NT
			AND TSECTION_TUWSEC.SSD_CF = TSECTION.SSD_CF
			AND TSECTION.SUPP_D = '9999-12-31'
	LEFT OUTER JOIN BI_<env>.TCONTR TCONTR ON
			TSECTION_TUWSEC.CTR_NF = TCONTR.CTR_NF
			AND TSECTION_TUWSEC.UWY_NF = TCONTR.UWY_NF
			AND TSECTION_TUWSEC.UW_NT = TCONTR.UW_NT
			AND TSECTION_TUWSEC.END_NT = TCONTR.END_NT
			AND TSECTION_TUWSEC.SSD_CF = TCONTR.SSD_CF
			AND TCONTR.CTRTYP_CT = 1
			AND TCONTR.SUPP_D = '9999-12-31'
	LEFT OUTER JOIN BI_<env>.TUWCTR TUWCTR ON
			(TSECTION_TUWSEC.CTR_NF = TUWCTR.CTR_NF
			AND TSECTION_TUWSEC.UWY_NF = TUWCTR.UWY_NF
			AND TSECTION_TUWSEC.UW_NT = TUWCTR.UW_NT
			AND TSECTION_TUWSEC.END_NT = TUWCTR.END_NT
			AND TSECTION_TUWSEC.SSD_CF = TUWCTR.SSD_CF
			AND TUWCTR.END_D = '9999-12-31'
			AND TUWCTR.SUPP_D = '9999-12-31' )
	INNER JOIN BI_<env>.TBANTECL TCONTR_TBANTECL3 ON
			(CAST( TCONTR.RENTYP_CT AS VARCHAR(5))= TCONTR_TBANTECL3.COLVAL_CT
			AND TCONTR_TBANTECL3.COL_LS = 'FA004'
			AND TCONTR_TBANTECL3.LAG_CF = 'E'
			AND TCONTR.CTRTYP_CT = 1
			AND TCONTR.SUPP_D = '9999-12-31')
	LEFT OUTER JOIN BI_<env>.TBANALL TSECTION_TBANALL3 ON
			(CAST(TSECTION_TUWSEC.SECQUA2_CF AS VARCHAR(5))= TSECTION_TBANALL3.COLVAL_CT
			AND TSECTION_TBANALL3.COL_LS = 'CTRQUA2_CF'
			AND TSECTION_TBANALL3.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TBANTECL TSECTION_TBANTECL ON
			(CAST(TSECTION.SECSTS_CT AS VARCHAR(5))= TSECTION_TBANTECL.COLVAL_CT
			AND TSECTION_TBANTECL.COL_LS = 'CTRSTS_CT'
			AND TSECTION_TBANTECL.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TBANTECL TSECTION_TBANTECL5 ON
			(CAST(TSECTION.ASSFINANCE_CT AS VARCHAR(5))= TSECTION_TBANTECL5.COLVAL_CT
			AND TSECTION_TBANTECL5.COL_LS = 'ASSFINANCE_CT'
			AND TSECTION_TBANTECL5.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TBANTECL TSECTION_TBANTECL6 ON
			(CAST(TSECTION_TUWSEC.USGAAP_CT AS VARCHAR(5))= TSECTION_TBANTECL6.COLVAL_CT
			AND TSECTION_TBANTECL6.COL_LS = 'USGAAP_CT'
			AND TSECTION_TBANTECL6.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TSOBL TSECTION_TSOBL ON
			(TSECTION.SOB_CF = TSECTION_TSOBL.SOB_CF
			AND TSECTION_TSOBL.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TGARL TSECTION_TGARL ON
			(TSECTION_TUWSEC.GAR_CF = TSECTION_TGARL.GAR_CF
			AND TSECTION_TGARL.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TTOPL TSECTION_TTOPL ON
			(TSECTION_TUWSEC.TOP_CF = TSECTION_TTOPL.TOP_CF
			AND TSECTION_TTOPL.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TLOBL TSECTION_TLOBL ON
			(TSECTION_TUWSEC.LOB_CF = TSECTION_TLOBL.LOB_CF
			AND TSECTION_TLOBL.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TCTRNATL TSECTION_TCTRNATL ON
			(TSECTION.NAT_CF = TSECTION_TCTRNATL.CTRNAT_CF
			AND TSECTION_TCTRNATL.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TBANTECL TCONTR_TBANTECL6 ON
			(CAST(TCONTR.CTRSTS_CT AS VARCHAR(5))= TCONTR_TBANTECL6.COLVAL_CT
			AND TCONTR_TBANTECL6.COL_LS = 'CTRSTS_CT'
			AND TCONTR_TBANTECL6.LAG_CF = 'E' )
	LEFT OUTER JOIN BI_<env>.TSECIFRS TSECIFRS ON 
		TSECTION_TUWSEC.CTR_NF=TSECIFRS.CTR_NF 
		AND TSECTION_TUWSEC.UWY_NF=TSECIFRS.UWY_NF 
		AND TSECTION_TUWSEC.SEC_NF=TSECIFRS.SEC_NF 
		AND TSECTION_TUWSEC.UW_NT=TSECIFRS.UW_NT 
		AND TSECTION_TUWSEC.SSD_CF=TSECIFRS.SSD_CF 
		AND TSECTION_TUWSEC.END_NT=TSECIFRS.END_NT
	LEFT OUTER JOIN BI_<env>.TBANALL TSECIFRS_TBANALL1 ON 
		(TSECIFRS.GRPIFRSTRA_CT = TSECIFRS_TBANALL1.COLVAL_CT
		AND  TSECIFRS_TBANALL1.COL_LS = 'IFRSTRA_CF'
		AND  TSECIFRS_TBANALL1.LAG_CF = 'E')
	LEFT OUTER JOIN BI_<env>.TBANALL TSECIFRS_TBANALL2 ON
		(TSECIFRS.GRPINIPRO_CF = TSECIFRS_TBANALL2.COLVAL_CT
		AND  TSECIFRS_TBANALL2.COL_LS = 'IFRSPRO_CF'
		AND  TSECIFRS_TBANALL2.LAG_CF = 'E')
	WHERE
		TSECTION_TUWSEC.SUBLEDGER_CF NOT IN (
			'05-01',
			'05-10',
			'06-01',
			'07-01',
			'10-01',
			'16-01',
			'18-16',
			'18-17',
			'20-01',
			'20-02',
			'26-01',
			'26-02',
			'26-03',
			'26-05',
			'26-06',
			'26-07',
			'26-08',
			'26-09',
			'26-10'
		)
		AND TSECTION_TUWSEC.ACTIV2_CF = '400'
		AND TCONTR.CTRTYP_CT = 1
	UNION
	SELECT
		'R' AS Assumed_Retro_FLAG,
		TUWRETSEC.RETCTR_NF,
		TUWRETSEC.RTY_NF,
		'0' AS END_NT,
		'1' AS UW_NT,
		TUWRETSEC.RETSEC_NF,
		TUWRETSEC.SSD_CF,
        TUWRETSEC.SUBLEDGER_CF,
		TO_CHAR(TUWRETSEC.CTRINC_D,'YYYYMMDD') AS Contract_Inception_date,
		TO_CHAR(TUWRETSEC.CTREXP_D,'YYYYMMDD') AS Contract_Expiry_date,
		NULL AS Duration_type,
		NULL AS Duration_type_lab,
		TRETCTR.RETCTRSTS_CT AS Contract_Status,
		TRETCTR_TBANTECL1.COLVAL_LS AS Contract_Status_lab,
		NULL AS Contract_nature,
		NULL AS Contract_NATURE_LAB,
		TUWRETSEC.LIFLOBR_CF AS External_LOB,
		TUWRETSEC.LIFLOBR_LL AS External_LOB_LAB,
		TUWRETSEC.LIFFINSOLR_CF AS Financial_Solution,
		TUWRETSEC.LIFFINSOLR_LL AS Financial_Solution_lab,
		TUWRETSEC.ASSFINANCE_CT AS IFRS,
		TSECTION_TBANTECL.COLVAL_LS AS IFRS_LAB,
		TUWRETSEC.LOB_CF AS LOB,
		TRETSEC_TLOBL.LOB_GS AS LOB_LAB,
		NULL AS Original_Inception_date,
		TUWRETSEC.LIFPRDLINR_CF AS Product_Line,
		TUWRETSEC.LIFPRDLINR_LL AS Product_Line_LAB,
		TUWRETSEC.SECQUA2_CF AS Qualifier_2_Section,
		TSECTION_TBANALL2.COLVAL_LS AS Qualifier_2_Section_LAB,
		NULL AS Section_Cancellation_date,
		NULL AS Section_Status,
		NULL AS Section_Inception_date,
		TUWRETSEC.SOB_CF,
		TRETSEC_TSOBL.SOB_GS,
		CAST(TUWRETSEC.SUBMRK_NT AS VARCHAR(6)) AS Market,
		TUWRETSEC.SUBMRK_LS AS Market_lab,
		TUWRETSEC.TOP_CF,
		TRETSEC_TTOPL.TOP_GS,
		NULL AS UNDERWRITING_UNIT,
		TRETSEC.RPOTRY_CF AS Analysis_Country,
		TUWRETSEC.GAR_CF,
		TRETSEC_TGARL.GAR_GS,
		TRETCTR.LIFTRTTYP_CF,
		TRETCTR_TBANALL1.COLVAL_LS,
		TUWRETSEC.NAT_CF,
		TRETSEC_TCTRNATL.CTRNAT_GS,
		NULL AS Cedent,
		TUWRETSEC.START_D,
		TUWRETSEC.END_D,
		TRETSEC.USGAAP_CT AS Term_Type,
		TSECTION_TBANTECL1.COLVAL_LM AS Term_Type_description,
		CASE WHEN TRETIFRS.GRPIFRSSEG_CT IS NULL 
			AND TRETIFRS.GRPIFRSSEG1_CT IS NULL 
			AND TRETIFRS.GRPINIPRO_CF IS NULL
		THEN NULL 
		ELSE (COALESCE(TRIM(TRETIFRS.GRPIFRSSEG_CT),'')||'-'||
				COALESCE(TRIM(TRETIFRS.GRPIFRSSEG1_CT),'')||'-'||
				COALESCE(TRIM(TRETIFRS.GRPINIPRO_CF),'')) 
		END AS IFRS17_Cohort,
		CASE WHEN TRETIFRS.GRPIFRSSEG_LL IS NULL 
			AND TRETIFRS.GRPIFRSSEG1_LL IS NULL 
			AND TRETIFRS_TBANALL2.COLVAL_LS IS NULL
		THEN NULL 
		ELSE (COALESCE(TRETIFRS.GRPIFRSSEG_LL,'')||'-'||
				COALESCE(TRETIFRS.GRPIFRSSEG1_LL,'')||'-'||
				COALESCE(TRETIFRS_TBANALL2.COLVAL_LS,'')) 
		END AS IFRS17_Cohort_LAB,
		TRETIFRS.GRPIFRSSEG_CT AS IFRS17_Portfolio,
		TRETIFRS.GRPIFRSSEG_LL AS IFRS17_Portfolio_LAB,
		TRETIFRS.GRPIFRSSEG1_CT AS IFRS17_Sub_Portfolio,
		TRETIFRS.GRPIFRSSEG1_LL AS IFRS17_Sub_Portfolio_LAB,
		TRETIFRS.GRPIFRSTRA_CT AS Transition_Mode,
		TRETIFRS_TBANALL1.COLVAL_LS AS Transition_Mode_LAB,
		TRETIFRS.GRPINIPRO_CF AS Initial_Profitability,
		TRETIFRS_TBANALL2.COLVAL_LS AS Initial_Profitability_LAB
	FROM
			BI_<env>.TBANTECL TRETCTR_TBANTECL1
	RIGHT OUTER JOIN BI_<env>.TRETCTR TRETCTR ON
			(CAST( TRETCTR.RETCTRSTS_CT AS VARCHAR(5))= TRETCTR_TBANTECL1.COLVAL_CT
			AND TRETCTR_TBANTECL1.COL_LS = 'RETCTRSTS_CT'
			AND TRETCTR_TBANTECL1.LAG_CF = 'E')
	INNER JOIN BI_<env>.TRETSEC TRETSEC ON
			(TRETCTR.RTY_NF = TRETSEC.RTY_NF
			AND TRETSEC.RETCTR_NF = TRETCTR.RETCTR_NF
			AND TRETSEC.SSD_CF = TRETCTR.SSD_CF
			AND TRETSEC.SUPP_D = '9999-12-31'
			AND TRETSEC.SUPP_D = '9999-12-31' )
	LEFT OUTER JOIN BI_<env>.TBANALL TSECTION_TBANALL2 ON
			(CAST(TRETSEC.SECQUA2_CF AS VARCHAR(5))= TSECTION_TBANALL2.COLVAL_CT
			AND TSECTION_TBANALL2.COL_LS = 'CTRQUA2_CF'
			AND TSECTION_TBANALL2.LAG_CF = 'E')
	LEFT OUTER JOIN BI_<env>.TBANTECL TSECTION_TBANTECL ON
			(CAST(TRETSEC.ASSFINANCE_CT AS VARCHAR(5))= TSECTION_TBANTECL.COLVAL_CT
			AND TSECTION_TBANTECL.COL_LS = 'ASSFINANCE_CT'
			AND TSECTION_TBANTECL.LAG_CF = 'E')
	LEFT OUTER JOIN BI_<env>.TBANTECL TSECTION_TBANTECL1 ON
			(CAST(TRETSEC.USGAAP_CT AS VARCHAR(5))= TSECTION_TBANTECL1.COLVAL_CT
			AND TSECTION_TBANTECL1.COL_LS = 'USGAAP_CT'
			AND TSECTION_TBANTECL1.LAG_CF = 'E')
	INNER JOIN BI_<env>.TUWRETSEC TUWRETSEC ON
			(TUWRETSEC.RETCTR_NF = TRETSEC.RETCTR_NF
			AND TUWRETSEC.RTY_NF = TRETSEC.RTY_NF
			AND TUWRETSEC.RETSEC_NF = TRETSEC.RETSEC_NF
			AND TUWRETSEC.SSD_CF = TRETSEC.SSD_CF
			AND TRETSEC.SUPP_D = '9999-12-31' )
	LEFT OUTER JOIN BI_<env>.TLOBL TRETSEC_TLOBL ON
			(TRETSEC_TLOBL.LOB_CF = TUWRETSEC.LOB_CF
			AND TRETSEC_TLOBL.LAG_CF = 'E')
	LEFT OUTER JOIN BI_<env>.TSOBL TRETSEC_TSOBL ON
			(TRETSEC_TSOBL.SOB_CF = TRETSEC.SOB_CF
			AND TRETSEC_TSOBL.LAG_CF = 'E')
	LEFT OUTER JOIN BI_<env>.TTOPL TRETSEC_TTOPL ON
			(TRETSEC_TTOPL.TOP_CF = TRETSEC.TOP_CF
			AND TRETSEC_TTOPL.LAG_CF = 'E')
	LEFT OUTER JOIN BI_<env>.TGARL TRETSEC_TGARL ON
			(TRETSEC_TGARL.GAR_CF = TUWRETSEC.GAR_CF
			AND TRETSEC_TGARL.LAG_CF = 'E')
	LEFT OUTER JOIN BI_<env>.TCTRNATL TRETSEC_TCTRNATL ON
			(TRETSEC_TCTRNATL.CTRNAT_CF = TRETSEC.NAT_CF
			AND TRETSEC_TCTRNATL.LAG_CF = 'E')
	LEFT OUTER JOIN BI_<env>.TBANALL TRETCTR_TBANALL1 ON
			(CAST(TRETCTR.LIFTRTTYP_CF AS VARCHAR(5))= TRETCTR_TBANALL1.COLVAL_CT
			AND TRETCTR_TBANALL1.COL_LS = 'LIFTRTTYP_CF'
			AND TRETCTR_TBANALL1.LAG_CF = 'E')
	LEFT JOIN BI_<env>.TRETIFRS TRETIFRS ON 
		(TUWRETSEC.RETCTR_NF=TRETIFRS.RETCTR_NF 
		AND TUWRETSEC.RTY_NF=TRETIFRS.RTY_NF)
	LEFT JOIN BI_<env>.TBANALL TRETIFRS_TBANALL1 ON
		(TRETIFRS.GRPIFRSTRA_CT = TRETIFRS_TBANALL1.COLVAL_CT
		AND TRETIFRS_TBANALL1.COL_LS = 'IFRSTRA_CF'
		AND TRETIFRS_TBANALL1.LAG_CF = 'E')
	LEFT JOIN BI_<env>.TBANALL TRETIFRS_TBANALL2 ON 
		(TRETIFRS.GRPINIPRO_CF = TRETIFRS_TBANALL2.COLVAL_CT
		AND TRETIFRS_TBANALL2.COL_LS = 'IFRSPRO_CF'
		AND TRETIFRS_TBANALL2.LAG_CF = 'E')
	WHERE( 
		TUWRETSEC.ACTIV2_CF = '400'
		AND SUBSTR(CAST((100 + TUWRETSEC.SSD_CF) AS CHAR(3)),2,2)|| '-' || 
		SUBSTR(CAST((100 + TUWRETSEC.ESB_CF) AS CHAR(3)),2,2) NOT IN (
			'05-01',
			'05-10',
			'06-01',
			'07-01',
			'10-01',
			'16-01',
			'18-16',
			'18-17',
			'20-01',
			'20-02',
			'26-01',
			'26-02',
			'26-03',
			'26-05',
			'26-06',
			'26-07',
			'26-08',
			'26-09',
			'26-10'
		) 
	);