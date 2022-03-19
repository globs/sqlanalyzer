CREATE OR REPLACE PROCEDURE SP_INIT_LOAD_OMEGA_DISCOUNT_CURRENCY (
	BIGINT,
	CHARACTER VARYING(64),
	CHARACTER VARYING(64)
) 
RETURNS INTEGER
language nzplsql
AS
BEGIN_PROC

DECLARE
	P_REQUEST_ID	ALIAS FOR $1;
    P_TRG_SCHEMA	ALIAS FOR $2;
    P_TRG_TABLE		ALIAS FOR $3;
	V_INS_DC		VARCHAR(ANY);
	V_INS_DWH		VARCHAR(ANY);
	V_WRK_TBL		VARCHAR(1000);
	V_HASH_KEY      VARCHAR(1000);
	V_PIVOT_KEY     VARCHAR(1000);
    V_SUPP_TS       TIMESTAMP;
	L_ERR_CD		CHAR(5);
	L_ERR_MSG		VARCHAR(32000);

BEGIN
	SET ISOLATION TO UR;
	
	V_WRK_TBL := P_TRG_TABLE || '_STG';
	V_SUPP_TS = '9999-12-31'; 

	DROP TABLE SESSION.KEYS IF EXISTS; 

	CREATE TEMP TABLE SESSION.KEYS (NAME VARCHAR(50), VALUE VARCHAR(1000)) DISTRIBUTE ON RANDOM;
	
	EXECUTE IMMEDIATE 'INSERT INTO SESSION.KEYS  
		SELECT
			CFK.KEY_NAME AS NAME, 
	    	''HASH(NVL(''||LISTAGG(
				CASE 
					WHEN C.TYPENAME = ''DATE'' THEN ''VARCHAR_FORMAT(''||CFK.COLUMN_NAME||'', ''''YYYYMMDD'''')'' 
					WHEN C.TYPENAME = ''TIMESTAMP'' THEN ''VARCHAR_FORMAT(''||CFK.COLUMN_NAME||'', ''''yyyymmddhh24miss'''')'' 
					ELSE CFK.COLUMN_NAME 
				END,'', ''''0'''')||''''-''''||NVL('') WITHIN GROUP(ORDER BY CFK.COLUMN_ORDER) ||'', ''''0''''), 2)'' AS VALUE 
		FROM DELIVERY_<env>.CONF_TABLE_KEY CFK
	    INNER JOIN SYSCAT.COLUMNS C
				ON C.COLNAME = CFK.COLUMN_NAME
				AND C.TABSCHEMA = ''' || P_TRG_SCHEMA || ''' 
				AND C.TABNAME = ''' || P_TRG_TABLE || ''' 
				AND C.COLNAME <> ''RANDOM_DISTRIBUTION_KEY''
		WHERE 
			TABLE_NAME = ''' || P_TRG_TABLE || '''
		GROUP BY 
			CFK.KEY_NAME WITH UR';
	
	SELECT VALUE INTO V_HASH_KEY FROM SESSION.KEYS WHERE NAME = 'HASH_KEY';
	SELECT VALUE INTO V_PIVOT_KEY FROM SESSION.KEYS WHERE NAME = 'PIVOT_KEY';
		
	EXECUTE IMMEDIATE 'DECLARE GLOBAL TEMPORARY TABLE SESSION. ' || V_WRK_TBL || '(
		CURRENCY_CODE			CHAR(3)		NOT NULL,
		DISCOUNT_CURRENCY_CODE	CHAR(3)		NOT NULL,
		LOB_CODE				CHAR(2),
		VALID_FROM				TIMESTAMP	NOT NULL,
		VALID_TO				TIMESTAMP	NOT NULL,
		REQUEST_ID				BIGINT		NOT NULL
	) ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE DISTRIBUTE ON RANDOM';

	V_INS_DC := 'INSERT INTO SESSION.' || V_WRK_TBL || ' (
		CURRENCY_CODE,
		DISCOUNT_CURRENCY_CODE,
		LOB_CODE,
		VALID_FROM,
		VALID_TO,
		REQUEST_ID
	)
	SELECT
		CUR_CF AS CURRENCY_CODE,
		GRPCUR_CF AS DISCOUNT_CURRENCY_CODE,
		''30'' AS LOB_CODE,
		CRE_D AS VALID_FROM,
		COALESCE(
			LEAD(CRE_D, 1) OVER (PARTITION BY CUR_CF ORDER BY CRE_D),
			''9999-12-31''
		) AS VALID_TO,
		' || P_REQUEST_ID || ' AS REQUEST_ID
	FROM
		BI_<env>.TCURSIILIFE
	UNION ALL
	SELECT
		CUR_CF AS CURRENCY_CODE,
		GRPCUR_CF AS DISCOUNT_CURRENCY_CODE,
		NULL AS LOB_CODE,
		CRE_D AS VALID_FROM,
		COALESCE(
			LEAD(CRE_D, 1) OVER (PARTITION BY CUR_CF ORDER BY CRE_D),
			''9999-12-31''
		) AS VALID_TO,
		' || P_REQUEST_ID || ' AS REQUEST_ID
	FROM
		BI_<env>.TCURSII';
	
	V_INS_DWH := 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
		CURRENCY_CODE,
		DISCOUNT_CURRENCY_CODE,
		LOB_CODE,
		HASH_KEY,  
		VALID_FROM,
		VALID_TO,
		PIVOT_KEY,
		SUPP_DATE,
		CREATED_REQUEST_ID,
		MODIFIED_REQUEST_ID,
		DELETED_REQUEST_ID
	)
	SELECT
		CURRENCY_CODE,
		DISCOUNT_CURRENCY_CODE,
		LOB_CODE,
		' || V_HASH_KEY || ' AS HASH_KEY,  
		VALID_FROM,
		VALID_TO,
		' || V_PIVOT_KEY || ' AS PIVOT_KEY,
		''' || V_SUPP_TS || ''' AS SUPP_DATE,
		REQUEST_ID AS CREATED_REQUEST_ID,
		NULL AS MODIFIED_REQUEST_ID,
		NULL AS DELETED_REQUEST_ID
	FROM
		SESSION.' || V_WRK_TBL || '';
	
	EXECUTE IMMEDIATE 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE CREATED_REQUEST_ID = ' || P_REQUEST_ID;

	RAISE NOTICE 'Executing V_INS_DC: ''%''', V_INS_DC;

	EXECUTE IMMEDIATE V_INS_DC;

	RAISE NOTICE 'Executing V_INS_DWH: ''%''', V_INS_DWH;

	EXECUTE IMMEDIATE V_INS_DWH;

	EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TBL || ' IF EXISTS';

	DROP TABLE SESSION.KEYS IF EXISTS;

EXCEPTION 
	WHEN OTHERS THEN 
		L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
		L_ERR_MSG := SQLERRM; 
		RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
		RETURN L_ERR_CD;

END;

END_PROC;