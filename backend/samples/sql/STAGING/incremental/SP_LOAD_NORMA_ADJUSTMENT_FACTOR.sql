SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_NORMA_ADJUSTMENT_FACTOR;

CREATE OR REPLACE PROCEDURE SP_LOAD_NORMA_ADJUSTMENT_FACTOR(
	BIGINT,
	CHARACTER VARYING(64),
	CHARACTER VARYING(64),
	CHARACTER VARYING(64),
	CHARACTER VARYING(64),
	SMALLINT,
	SMALLINT
) 
RETURNS INTEGER
language nzplsql
AS
BEGIN_PROC

DECLARE
	P_REQUEST_ID        ALIAS FOR $1;
    P_SRC_SCHEMA        ALIAS FOR $2;
    P_SRC_TABLE         ALIAS FOR $3;
    P_TRG_SCHEMA        ALIAS FOR $4; 
    P_TRG_TABLE         ALIAS FOR $5;    
    P_FILEYEAR    	    ALIAS FOR $6;
	P_FILEQUARTER       ALIAS FOR $7;
	V_LOAD_TEMP_WRK_TBL	VARCHAR(ANY);
	V_LOAD_WRK_TBL		VARCHAR(ANY);
	V_WRK_TBL		    VARCHAR(1000);
	L_ERR_CD		    CHAR(5);
	L_ERR_MSG		    VARCHAR(32000);

BEGIN
	SET ISOLATION TO UR;

	V_WRK_TBL := P_TRG_TABLE || '_' || P_REQUEST_ID;
	
	EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TBL || ' IF EXISTS';
	
	EXECUTE IMMEDIATE 'DECLARE GLOBAL TEMPORARY TABLE SESSION. ' || V_WRK_TBL || '(
		REPORTING_BASIS_ID SMALLINT,
		CLOSING_DATE DATE NOT NULL,
		PARAMETER_TYPE_ID SMALLINT NOT NULL,
		SEGMENT_TYPE_ID SMALLINT,
		SEGMENT_CODE VARCHAR(16),
		CONTRACT_NATURE_CODE CHAR(1),
		SUBSIDIARY_CODE SMALLINT,
		SUBLEDGER_CODE SMALLINT,
		LOB_CODE CHAR(2),
		DOMAIN_CODE VARCHAR(16),
		PLAN_CATEGORY_ID SMALLINT,
		UNDERWRITING_YEAR SMALLINT,
		CURRENCY_CODE CHAR(3),
		FACTOR_TYPE_ID SMALLINT,
		FACTOR DECFLOAT NOT NULL,
		REQUEST_ID BIGINT
	) ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE DISTRIBUTE ON RANDOM';

	V_LOAD_TEMP_WRK_TBL := 'INSERT INTO SESSION.' || V_WRK_TBL || ' (
		REPORTING_BASIS_ID,
	    CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID,
	    SEGMENT_CODE,
	    CONTRACT_NATURE_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    PLAN_CATEGORY_ID,
		UNDERWRITING_YEAR,
	    CURRENCY_CODE,
	    FACTOR_TYPE_ID,
		FACTOR,
	    REQUEST_ID
	)
	SELECT
		rb.ID AS REPORTING_BASIS_ID,
		DATE(ADD_MONTHS(TO_DATE(''01/01/''||'|| P_FILEYEAR||',''DD/MM/YYYY''), '|| P_FILEQUARTER||' * 3) -1) AS CLOSING_DATE,
		3 AS PARAMETER_TYPE_ID,
		2 AS SEGMENT_TYPE_ID,
		CAST(LOB_N1 as CHAR(16)) AS SEGMENT_CODE,
		CASE 
			WHEN NATURE = '''' OR NATURE IS NULL THEN NULL 
			ELSE CAST(NATURE AS CHAR(1)) 
		END AS CONTRACT_NATURE_CODE,
		CASE 
			WHEN SUBSIDIARY = '''' OR SUBSIDIARY IS NULL THEN NULL 
			ELSE SUBSIDIARY 
		END AS SUBSIDIARY_CODE,
		CASE 
			WHEN SUBLEDGER = '''' OR SUBLEDGER IS NULL THEN NULL 
			ELSE SUBLEDGER END AS SUBLEDGER_CODE,
		NULL AS LOB_CODE,
		CAST(DOMAIN_CODE AS CHAR(16)) AS DOMAIN_CODE,
		NULL AS PLAN_CATEGORY_ID,
		NULL AS UNDERWRITING_YEAR,
		NULL AS CURRENCY_CODE,
		1 AS FACTOR_TYPE_ID,
	    CAST(TRIM(REPLACE(PREMIUM, CHR(13), '''')) AS DOUBLE) AS FACTOR,
	    ' || P_REQUEST_ID || ' AS REQUEST_ID
	FROM 
		' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || '
		INNER JOIN
			DWHD1_<env>.DWH_REPORTING_BASIS rb
			ON TRIM(UPPER(NORM)) = TRIM(UPPER(rb.CODE))
	UNION ALL
	SELECT
		rb.ID AS REPORTING_BASIS_ID,
		DATE(ADD_MONTHS(TO_DATE(''01/01/''||'|| P_FILEYEAR||',''DD/MM/YYYY''), '|| P_FILEQUARTER||' * 3) -1) AS CLOSING_DATE,
		3 AS PARAMETER_TYPE_ID,
		2 AS SEGMENT_TYPE_ID,
		CAST(LOB_N1 as CHAR(16)) AS SEGMENT_CODE,
		CASE 
			WHEN NATURE = '''' OR NATURE IS NULL THEN NULL 
			ELSE CAST(NATURE AS CHAR(1)) 
		END AS CONTRACT_NATURE_CODE,
		CASE 
			WHEN SUBSIDIARY = '''' OR SUBSIDIARY IS NULL THEN NULL 
			ELSE SUBSIDIARY 
		END AS SUBSIDIARY_CODE,
		CASE 
			WHEN SUBLEDGER = '''' OR SUBLEDGER IS NULL THEN NULL 
			ELSE SUBLEDGER END AS SUBLEDGER_CODE,
		NULL AS LOB_CODE,
		CAST(DOMAIN_CODE AS CHAR(16)) AS DOMAIN_CODE,
		NULL AS PLAN_CATEGORY_ID,
		NULL AS UNDERWRITING_YEAR,
		NULL AS CURRENCY_CODE,
		2 AS FACTOR_TYPE_ID,
		CAST(TRIM(REPLACE(RESERVES, CHR(13), '''')) AS DOUBLE) AS FACTOR,
		' || P_REQUEST_ID || ' AS REQUEST_ID
	FROM 
		' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || '
		INNER JOIN
			DWHD1_<env>.DWH_REPORTING_BASIS rb
			ON TRIM(UPPER(NORM)) = TRIM(UPPER(rb.CODE))';
	
	V_LOAD_WRK_TBL := 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
	    REPORTING_BASIS_ID,
	    CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID,
	    SEGMENT_CODE,
	    CONTRACT_NATURE_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    PLAN_CATEGORY_ID,
		UNDERWRITING_YEAR,
	    CURRENCY_CODE,
	    FACTOR_TYPE_ID,
		FACTOR,
	    REQUEST_ID
	)
	SELECT 
		REPORTING_BASIS_ID,
	    CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID,
	    SEGMENT_CODE,
	    CONTRACT_NATURE_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    PLAN_CATEGORY_ID,
		UNDERWRITING_YEAR,
	    CURRENCY_CODE,
	    FACTOR_TYPE_ID,
		FACTOR,
	    REQUEST_ID
	FROM   
		SESSION.' || V_WRK_TBL || '
	WHERE 
		CONTRACT_NATURE_CODE IS NOT NULL  
		AND SUBSIDIARY_CODE IS NOT NULL 
		AND SUBLEDGER_CODE IS NOT NULL
	UNION ALL
	SELECT 
		REPORTING_BASIS_ID,
	    CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID,
	    SEGMENT_CODE,
	    CONTRACT_NATURE_CODE,
	    TU.SSD_CF AS SUBSIDIARY_CODE,
    	TU.ESB_CF AS SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    PLAN_CATEGORY_ID,
		UNDERWRITING_YEAR,
	    CURRENCY_CODE,
	    FACTOR_TYPE_ID,
		FACTOR,
	    REQUEST_ID
	FROM   
		SESSION.' || V_WRK_TBL || '
		CROSS JOIN  
			( 
				SELECT 
					SSD_CF,
					ESB_CF 
				FROM 
					BI_<env>.TESB a
				WHERE 
					LIFE_CF = 2
			) TU
	WHERE 
		CONTRACT_NATURE_CODE = ''F''
		AND SUBSIDIARY_CODE IS NULL 
		AND SUBLEDGER_CODE IS NULL 
		AND REPORTING_BASIS_ID = 2
	UNION ALL
	SELECT 
		REPORTING_BASIS_ID,
	    CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID,
	    SEGMENT_CODE,
	    ''N'' AS CONTRACT_NATURE_CODE,
	    TU.SSD_CF AS SUBSIDIARY_CODE,
	    TU.ESB_CF AS SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    PLAN_CATEGORY_ID,
		UNDERWRITING_YEAR,
	    CURRENCY_CODE,
	    FACTOR_TYPE_ID,
		FACTOR,
	    REQUEST_ID
	FROM 
		SESSION.' || V_WRK_TBL || '
		CROSS JOIN  
			( 
				SELECT  
					SSD_CF,
					ESB_CF  
				FROM 
					BI_<env>.TESB a 
				WHERE  
					LIFE_CF = 2
			) TU
	WHERE  
		SEGMENT_CODE = 3021 
		AND REPORTING_BASIS_ID = 2 
		AND CONTRACT_NATURE_CODE IS NULL
		AND SUBSIDIARY_CODE IS NULL 
		AND SUBLEDGER_CODE IS NULL 
	UNION
	SELECT 
		af.REPORTING_BASIS_ID,
	    af.CLOSING_DATE,
	    af.PARAMETER_TYPE_ID,
	    af.SEGMENT_TYPE_ID,
	    SC.LOB AS SEGMENT_CODE,
	    CN.CONTRACT_NATURE_CODE,
	    TU.SSD_CF AS SUBSIDIARY_CODE,
	    TU.ESB_CF AS SUBLEDGER_CODE,
	    af.LOB_CODE,
	    ''RetroNP'' AS DOMAIN_CODE,
	    af.PLAN_CATEGORY_ID,
		af.UNDERWRITING_YEAR,
	    af.CURRENCY_CODE,
	    af.FACTOR_TYPE_ID,
		af.FACTOR,
	    af.REQUEST_ID
	FROM
		SESSION.' || V_WRK_TBL || ' AS af
		CROSS JOIN 
			( 
				SELECT  
					SSD_CF,
					ESB_CF    
				FROM 
					BI_<env>.TESB 
				WHERE 
					LIFE_CF = 2
			) TU 
		CROSS JOIN 
			(
				SELECT 
					TRIM(C.SGMT_LS) AS LOB 
				FROM 
					BI_<env>.TSEGMENTATION B 
					JOIN 
						BI_<env>.TSEGMT C 
						ON B.SGT_NT = C.SGT_NT 
						AND B.SGTVER_NT = C.SGTVER_NT 
				WHERE 
					B.SGTTYP_NT = 2 
					AND B.SGTVER_NT = (
						SELECT 
							MAX(SGTVER_NT) 
						FROM 
							BI_<env>.TSEGMENTATION 
						WHERE 
							SGTTYP_NT = 2
					) 
					AND C.sgtlvl_nt = 0 
					AND TRIM(C.SGMT_LS) < ''5000'' 
					AND TRIM(C.SGMT_LS) <> ''0'' 
				GROUP BY 
					C.SGMT_LS
			) SC
		CROSS JOIN 
			(
				SELECT 
					''P'' AS CONTRACT_NATURE_CODE 
				FROM 
					SYSIBM.SYSDUMMY1 
				UNION ALL 
				SELECT 
					''N'' AS CONTRACT_NATURE_CODE  
				FROM SYSIBM.SYSDUMMY1
				UNION ALL 
				SELECT 
					''F'' AS CONTRACT_NATURE_CODE  
				FROM SYSIBM.SYSDUMMY1
			) CN
	WHERE  
		af.SEGMENT_CODE = 3011 
		AND af.REPORTING_BASIS_ID = 2
		AND af.CONTRACT_NATURE_CODE IS NULL
		AND af.SUBSIDIARY_CODE IS NULL 
		AND af.SUBLEDGER_CODE IS NULL 
		AND NOT (
			SC.LOB = ''3021'' 
			AND CN.CONTRACT_NATURE_CODE = ''N''
		)';
	
	EXECUTE IMMEDIATE 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE REQUEST_ID = ' || P_REQUEST_ID;

	RAISE NOTICE 'Executing V_LOAD_TEMP_WRK_TBL: ''%''', V_LOAD_TEMP_WRK_TBL;

	EXECUTE IMMEDIATE V_LOAD_TEMP_WRK_TBL;

	RAISE NOTICE 'Executing V_LOAD_WRK_TBL: ''%''', V_LOAD_WRK_TBL;

	EXECUTE IMMEDIATE V_LOAD_WRK_TBL;

	EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TBL || ' IF EXISTS';

EXCEPTION 
	WHEN OTHERS THEN 
		L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
		L_ERR_MSG := SQLERRM; 
		RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
		RETURN L_ERR_CD;

END;

END_PROC;