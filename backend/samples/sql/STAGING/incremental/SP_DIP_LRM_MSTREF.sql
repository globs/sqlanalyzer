SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_DIP_LRM_MSTREF;

CREATE OR REPLACE PROCEDURE SP_DIP_LRM_MSTREF(
	 CHARACTER VARYING(50)
	,CHARACTER VARYING(50)
	,CHARACTER VARYING(50)
	,CHARACTER VARYING(50)
	,CHARACTER VARYING(50)
) RETURNS INTEGER
LANGUAGE NZPLSQL AS 
BEGIN_PROC
DECLARE 
	P_STG_SCHEMA            ALIAS FOR $1;
	P_WRK_MASTER_REF_DATA   ALIAS FOR $2;
	P_WRK_FX_RATE_TBL       ALIAS FOR $3;   
	P_WRK_TBL_YC            ALIAS FOR $4;
	P_QTR_DATE              ALIAS FOR $5;
	V_CURR_TS               VARCHAR(50);
	V_SQL1                  VARCHAR(ANY);
	V_SQL2                  VARCHAR(ANY);
	V_SQL3                  VARCHAR(ANY);
	V_STG_SELECT            VARCHAR(ANY);
	V_STG_SELECT_2          VARCHAR(ANY);
	V_STG_SELECT_3          VARCHAR(ANY);
	V_QTRS                  VARCHAR(50);
	V_QTR                   VARCHAR(50);
	V_YR                    VARCHAR(50);	
	L_ERR_CD                CHAR(5);
	L_ERR_MSG               VARCHAR(32000);    
    
BEGIN
SET ISOLATION TO UR;

V_QTR = SUBSTRING(P_QTR_DATE,2,1); 
V_YR = SUBSTRING(P_QTR_DATE,3,4);
V_CURR_TS := CURRENT_TIMESTAMP;

V_QTRS :=
CASE
   WHEN V_QTR = 4 THEN '(''Q4' ||(V_YR)|| ''')'
   WHEN V_QTR = 1 THEN '(''Q4' ||(V_YR-1)|| ''',''Q1' || V_YR || ''')'
   WHEN V_QTR = 2 THEN '(''Q4' ||(V_YR-1)|| ''',''Q1' || V_YR || ''',''Q2' || V_YR || ''')'
   WHEN V_QTR = 3 THEN '(''Q4' ||(V_YR-1)|| ''',''Q1' || V_YR || ''',''Q2' || V_YR || ''',''Q3' || V_YR || ''')'
END ;

RAISE NOTICE '''%''',
V_QTRS ;

V_STG_SELECT ='(
SELECT
		"Reference_Data",
		Code AS "Code",
		Name AS "Name"
FROM(
	SELECT
		''Subsidiary'' AS "Reference_Data" ,
		TRIM(CAST(SSD_CF AS VARCHAR(64))) AS Code ,
		SSD_LS AS Name
	FROM
		 BI_<env>.TSUBSID
	UNION ALL
		SELECT
			''Ledger'' AS "Reference_Data" ,
			TRIM(CAST(SSD_CF AS VARCHAR(64))) || ''-'' || TRIM(CAST(ESB_CF AS VARCHAR(64))) AS CODE ,
			ESB_LS AS NAME
		FROM
			 BI_<env>.TESB
	UNION ALL
		SELECT
			''Region'' AS "Reference_Data" ,
			TRIM(CAST(CTYSUP_CF AS VARCHAR(64))) AS Code ,
			CTYSUP_LS AS Name
		FROM
			 BI_<env>.TCTYSUPL
		WHERE
			LAG_CF = ''E''
	UNION ALL
		SELECT
			''Treaty_Life_Characteristic'' AS "Reference_Data" ,
			TRIM(CAST(COLVAL_CT AS VARCHAR(64))) AS Code ,
			COLVAL_LM AS Name
		FROM
			 BI_<env>.TBANALL
		WHERE
			COL_LS = ''LIFTRTTYP_CF''
			AND LAG_CF = ''E''
	UNION ALL
		SELECT
			''Financing_Type'' AS "Reference_Data" ,
			TRIM(CAST(COLVAL_CT AS VARCHAR(64))) AS Code ,
			COLVAL_LM AS Name
		FROM
			 BI_<env>.TBANALL
		WHERE
			COL_LS = ''FINTYP_CF''
			AND LAG_CF = ''E''
	UNION ALL
		SELECT
			''IAS39'' AS "Reference_Data" ,
			TRIM(CAST(COLVAL_CT AS VARCHAR(64))) AS Code ,
			COLVAL_LM AS Name
		FROM
			 BI_<env>.TBANTECL
		WHERE
			COL_LS = ''ASSFINANCE_CT''
			AND LAG_CF = ''E''
	UNION ALL
		SELECT
			''USGAAP'' AS "Reference_Data" ,
			TRIM(CAST(COLVAL_CT AS VARCHAR(64))) AS Code ,
			COLVAL_LM AS Name
		FROM
			 BI_<env>.TBANTECL
		WHERE
			COL_LS = ''USGAAP_CT''
			AND LAG_CF = ''E''
	UNION ALL
		SELECT
			''Coinsurance'' AS "Reference_Data" ,
			TRIM(CAST(SOB_CF AS VARCHAR(64))) AS Code ,
			SOB_GL AS Name
		FROM
			BI_<env>.TSOB
	UNION ALL
		SELECT
			''Type_Of_Business'' AS "Reference_Data" ,
			TRIM(CAST(80 AS VARCHAR(64))) AS Code ,
			''YRT'' AS Name
		FROM
			SYSIBM.SYSDUMMY1
	UNION ALL
		SELECT
			''Type_Of_Business'' AS "Reference_Data" ,
			TRIM(CAST(82 AS VARCHAR(64))) AS Code ,
			''COINS'' AS Name
		FROM
			SYSIBM.SYSDUMMY1
	UNION ALL
		SELECT
			''Omega_LoB'' AS "Reference_Data" ,
			TRIM(CAST(LOB_CF AS VARCHAR(64))) AS Code ,
			LOB_GL AS Name
		FROM
			BI_<env>.TLOBL
		WHERE
			LAG_CF = ''E''
	UNION ALL
		SELECT
			''Guarantee'' AS "Reference_Data" ,
			TRIM(CAST(GAR_CF AS VARCHAR(64))) AS Code ,
			GAR_GS AS Name
		FROM
			BI_<env>.TGARL
		WHERE
			LAG_CF = ''E''
	UNION ALL
		SELECT
			''Policy_Type'' AS "Reference_Data" ,
			TRIM(CAST(TOP_CF AS VARCHAR(64))) AS Code ,
			TOP_GL AS Name
		FROM
			BI_<env>.TTOPL
		WHERE
			LAG_CF = ''E''
	UNION ALL
		SELECT
			''Nature'' AS "Reference_Data" ,
			TRIM(CAST(CTRNATMNE_HD AS VARCHAR(64))) AS Code ,
			CTRNAT_GL AS Name
		FROM
			BI_<env>.TCTRNATL
		WHERE
			LAG_CF = ''E''
	UNION ALL
		SELECT
			''Actuarial_Model_Type'' AS "Reference_Data" ,
			TRIM(CAST(COLVAL_CT AS VARCHAR(64))) AS Code ,
			COLVAL_LM AS Name
		FROM
			BI_<env>.TBANTECL
		WHERE
			COL_LS = ''DATATYP_CT''
			AND LAG_CF = ''E''
	UNION ALL
		SELECT
			DISTINCT ''Client'' AS "Reference_Data" ,
			TRIM(CAST(CLI_NF AS VARCHAR(64))) AS Code,
			REPLACE(CLISHONAM_LD, '';'', '' '') AS Name
		FROM
			BI_<env>.TCLIENT
	UNION ALL
		SELECT
			''Term_Type'' AS "Reference_Data" ,
			TRIM(CAST(COLVAL_CT AS VARCHAR(64))) AS Code ,
			COLVAL_LM AS Name
		FROM
			BI_<env>.TBANALL
		WHERE
			COL_LS = ''POLDUR_CF''
			AND LAG_CF = ''E''
	UNION ALL
		SELECT
			''Business_Maturity'' AS "Reference_Data" ,
			TRIM(CAST(CODE AS VARCHAR(64))) AS Code ,
			NAME AS Name
		FROM
			 BI_<env>.BUSINESS_MATURITY
	UNION ALL
		SELECT
			''Market_Unit'' AS "Reference_Data" ,
			TRIM(CAST(COLVAL_CT AS VARCHAR(64))) AS Code ,
			COLVAL_LM AS Name
		FROM
			BI_<env>.TBANTECL
		WHERE
			COL_LS = ''SUBMRK_NT''
			AND LAG_CF = ''E''
	UNION ALL
		SELECT
			''Currency'' AS "Reference_Data" ,
			TRIM(CAST(CUR_CF AS VARCHAR(64))) AS Code ,
			CUR_LL AS Name
		FROM
			BI_<env>.TCURL
		WHERE
			LAG_CF = ''E''
	UNION ALL
		SELECT
			''CSM_Cashflow_Legs'' AS "Reference_Data" ,
			TRIM(CAST(CODE AS VARCHAR(64))) AS Code ,
			NAME AS Name
		FROM
			BI_<env>.POSITION
		WHERE
			POSITION_TYPE_ID = 2
			AND VALID_TO = ''9999-12-31''
	UNION ALL
		SELECT
			''Scenario_Type'' AS "Reference_Data" ,
			TRIM(CAST(CODE AS VARCHAR(64))) AS Code ,
			NAME AS Name
		FROM
			 BI_<env>.SCENARIO_TYPE
	UNION ALL
		SELECT
			''Level_Of_Analysis'' AS "Reference_Data" ,
			TRIM(CAST(CODE AS VARCHAR(64))) AS Code ,
			NAME AS Name
		FROM
			BI_<env>.LEVEL_OF_ANALYSIS
		WHERE
			VALID_TO = ''9999-12-31''
	UNION ALL
		SELECT
			''Period'' AS "Reference_Data" ,
			TRIM(CAST(ID AS VARCHAR(64))) AS Code ,
			NAME AS Name
		FROM
			BI_<env>.PERIOD_TYPE
	UNION ALL
		SELECT
			''Type_Of_Rate'' AS "Reference_Data" ,
			TRIM(CAST(''F'' AS VARCHAR(64))) AS Code ,
			''Financial_Rate'' AS Name
		FROM
			SYSIBM.SYSDUMMY1
	UNION ALL
		SELECT
			''Type_Of_Rate'' AS "Reference_Data" ,
			TRIM(CAST(''M'' AS VARCHAR(64))) AS Code ,
			''Average_Rate'' AS Name
		FROM
			SYSIBM.SYSDUMMY1
	UNION ALL
		SELECT
			''Type_Of_Rate'' AS "Reference_Data" ,
			TRIM(CAST(''T'' AS VARCHAR(64))) AS Code ,
			''Quarterly_Rate'' AS Name
		FROM
			SYSIBM.SYSDUMMY1
	UNION ALL
		SELECT
			''Type_Of_Rate'' AS "Reference_Data" ,
			TRIM(CAST(''C'' AS VARCHAR(64))) AS Code ,
			''Closing_Rate'' AS Name
		FROM
			SYSIBM.SYSDUMMY1
	UNION ALL
		SELECT
			''Maturity_Type'' AS "Reference_Data" ,
			TRIM(CAST(CODE AS VARCHAR(64))) AS Code ,
			NAME AS Name
		FROM
			BI_<env>.MATURITY_TYPE
	UNION ALL
		SELECT
			''Yield_Curve_Type'' AS "Reference_Data" ,
			TRIM(CAST(CODE AS VARCHAR(64))) AS Code ,
			NAME AS Name
		FROM
			BI_<env>.PARAMETER_TYPE
	UNION ALL
		SELECT
			''Reporting_Basis'' AS "Reference_Data" ,
			TRIM(CAST(CODE AS VARCHAR(64))) AS Code ,
			NAME AS Name
		FROM
			BI_<env>.REPORTING_BASIS
			UNION ALL
		SELECT
			''SPLIT'' AS  "Reference_Data" ,
			CODE  AS Code ,
			NAME AS Name
		FROM 
		BI_<env>.SPLIT_TYPE
	) 
)';
					 
V_SQL1 = 'DELETE  FROM ' || P_STG_SCHEMA || '.' || P_WRK_MASTER_REF_DATA ;
RAISE NOTICE 'Executing V_SQL1: ''%''', V_SQL1;
EXECUTE IMMEDIATE V_SQL1;

V_SQL1 = 'INSERT INTO ' || P_STG_SCHEMA || '.' || P_WRK_MASTER_REF_DATA || ' ' || V_STG_SELECT;
RAISE NOTICE 'Executing V_SQL1: ''%''', V_SQL1;
EXECUTE IMMEDIATE V_SQL1;


V_STG_SELECT_3 = '
SELECT 
    MT_CODE
	,MATURITY
	,CURRENCY_CODE
	,VALID_FROM
	,JOBH
	,AB
	,PT_CODE
	,'''|| V_CURR_TS ||'''
	,RATE
FROM (SELECT 
        x.MT_CODE
        ,x.MATURITY
        ,x.CURRENCY_CODE
        ,x.VALID_FROM
        ,x.JOBH
        ,''Q''||QUARTER(x.CLOSING_DATE) AS QTR
        ,YEAR(x.CLOSING_DATE) AS CLOSING_YR
        ,x.AB
        ,x.PT_CODE
        ,x.RATE
	FROM (
		SELECT 
			EC.RATE,
			MT.CODE AS MT_CODE,
			EC.MATURITY,
			EC.VALID_FROM,
						EC.CLOSING_DATE,
			ROW_NUMBER() OVER(
				PARTITION BY 
					EC.RATE,MT.CODE
					,EC.MATURITY
					,EC.CURRENCY_CODE
					,PT.CODE
					,EC.CLOSING_DATE 
				ORDER BY EC.RATE 
			) AS EE,
			YEAR(EC.ECONOMIC_DATA_AS_OF_DATE)|| ''Q''|| QUARTER(EC.ECONOMIC_DATA_AS_OF_DATE)|| PT.CODE AS JOBH,						
			EC.CURRENCY_CODE,
			VARCHAR_FORMAT(EC.CLOSING_DATE,''YYYY-MM-DD'')||''_''||PT.CODE AS AB,						
			PT.CODE AS PT_CODE
		FROM BI_<env>.ECONOMIC_RATE AS EC  
		INNER JOIN BI_<env>.PARAMETER_TYPE AS PT ON EC.PARAMETER_TYPE_ID = PT.ID  AND PT.CODE = ''ZCR'' 
		INNER JOIN BI_<env>.MATURITY_TYPE AS MT  ON EC.MATURITY_TYPE_ID = MT.ID 
        WHERE EC.LOB_CODE IN (''30'', ''31'') AND EC.VALID_TO = ''9999-12-31''
	)X 
		WHERE EE = 1
) WHERE QTR||CLOSING_YR IN ' || V_QTRS || '
';

V_SQL3 = 'DELETE  FROM ' || P_STG_SCHEMA || '.' || P_WRK_TBL_YC ;
RAISE NOTICE 'Executing V_SQL3: ''%''', V_SQL3;
EXECUTE IMMEDIATE V_SQL3;

V_SQL3 = 'INSERT INTO ' || P_STG_SCHEMA || '.' || P_WRK_TBL_YC || ' ' || V_STG_SELECT_3;
RAISE NOTICE 'Executing V_SQL3: ''%''', V_SQL3;
EXECUTE IMMEDIATE V_SQL3;

V_STG_SELECT_2 = '(
SELECT
	"Currency_From",
	"Currency_To",
	"Modified_Date",
	"As_Of_Date",
	"Name",
	"Type_Of_Rate",
	"Reference_Valid_From",
	"Exchange_Rate"
FROM (
	SELECT
		a.CUR_CF AS "Currency_From" ,
		b.SSDCUR_CF AS "Currency_To" ,
		a.START_D AS "Modified_Date" ,
		a.EXC_D AS "As_Of_Date" ,
		VARCHAR_FORMAT(a.EXC_D,''YYYY-MM-DD'')|| ''-'' || a.EXC_CT || ''-'' || a.CUR_CF AS "Name",
		a.EXC_CT AS "Type_Of_Rate" ,
		''Q''||QUARTER(a.EXC_D) AS QTR,
        YEAR(a.EXC_D) AS CLOSING_YR,
		CURRENT_TIMESTAMP AS "Reference_Valid_From",
		a.EXC_R AS "Exchange_Rate"
	FROM BI_<env>.TBOCURQUOT2_H a
	INNER JOIN BI_<env>.TSUBSID b ON a.SSD_CF = b.SSD_CF AND a.EXC_CT = ''C'' AND a.END_D = a.SUPP_D AND a.END_D = ''9999-12-31''
	GROUP BY
		a.EXC_R,
		a.CUR_CF,
		b.SSDCUR_CF,
		a.START_D,
		a.EXC_D,
		a.EXC_CT
    )
WHERE QTR||CLOSING_YR IN ' || V_QTRS || '
)';

V_SQL2 = 'DELETE  FROM ' || P_STG_SCHEMA || '.' || P_WRK_FX_RATE_TBL ;
RAISE NOTICE 'Executing V_SQL2: ''%''', V_SQL2;
EXECUTE IMMEDIATE V_SQL2;

V_SQL2 = 'INSERT INTO ' || P_STG_SCHEMA || '.' || P_WRK_FX_RATE_TBL || ' ' || V_STG_SELECT_2;
RAISE NOTICE 'Executing V_SQL2: ''%''', V_SQL2;
EXECUTE IMMEDIATE V_SQL2;


EXCEPTION 
	WHEN OTHERS THEN 
		L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
		L_ERR_MSG := SQLERRM; 
		RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
		RETURN L_ERR_CD;
END;
END_PROC;