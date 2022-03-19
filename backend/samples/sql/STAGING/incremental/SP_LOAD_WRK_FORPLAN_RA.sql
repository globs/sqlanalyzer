SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_WRK_FORPLAN_RA;

CREATE OR REPLACE PROCEDURE SP_LOAD_WRK_FORPLAN_RA (
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
    P_REQUEST_ID			ALIAS FOR $1;
    P_SRC_SCHEMA        	ALIAS FOR $2;
    P_SRC_TABLE         	ALIAS FOR $3;
    P_TRG_SCHEMA        	ALIAS FOR $4; 
    P_TRG_TABLE         	ALIAS FOR $5;    
    P_FILEYEAR    	    	ALIAS FOR $6;
	P_FILEQUARTER       	ALIAS FOR $7;
	V_LOAD_WKR_FORPLAN_RA	VARCHAR(ANY);
	L_ERR_CD		    	CHAR(5);
	L_ERR_MSG		    	VARCHAR(32000);

BEGIN
	SET ISOLATION TO UR;

	V_LOAD_WKR_FORPLAN_RA := 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
		UWY,
		LOB_N1,
		NATURE,
		RA_FUTURE,
		RA_PAST
	)
	SELECT DISTINCT
		crc.UWY,
		LOBN1 AS LOB_N1,
		NATURE,
		SUM(PREMIUM) OVER (PARTITION BY crc.UWY, LOBN1, NATURE, NORM) AS RA_FUTURE,
		SUM(RESERVE) OVER (PARTITION BY crc.UWY, LOBN1, NATURE, NORM) AS RA_PAST
	FROM  
		(
			SELECT  
				SEGMENT_CODE AS LOBN1,
				CONTRACT_NATURE_CODE AS NATURE,
				rb.CODE AS NORM,
				CASE 
					WHEN FACTOR_TYPE_ID = 1 THEN FACTOR 
				END AS PREMIUM,
				CASE 
					WHEN FACTOR_TYPE_ID = 2 THEN FACTOR 
				END AS RESERVE
			FROM
				' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || '
				JOIN
					DWHD1_<env>.DWH_REPORTING_BASIS rb
					ON REPORTING_BASIS_ID = rb.ID
			WHERE
				(
					MODIFIED_REQUEST_ID = ' || P_REQUEST_ID || '
					OR (
						CREATED_REQUEST_ID = ' || P_REQUEST_ID || '
						AND MODIFIED_REQUEST_ID IS NULL
					)
				)
				AND DELETED_REQUEST_ID IS NULL
				AND PARAMETER_TYPE_ID = 16
				AND VALID_TO = ''9999-12-31-00.00.00.000000''
				AND CLOSING_DATE = DATE(ADD_MONTHS(TO_DATE(''01/01/''||'|| P_FILEYEAR||',''DD/MM/YYYY''), '|| P_FILEQUARTER||' * 3) - 1)
		)
		CROSS JOIN 
			(
				SELECT  
					''N'' AS UWY 
				FROM 
					SYSIBM.sysdummy1
				UNION ALL 
				SELECT  
					''N+1'' AS UWY 
				FROM 
					SYSIBM.sysdummy1
				UNION ALL 
				SELECT  
					''N+2'' AS UWY 
				FROM 
					SYSIBM.sysdummy1
			) crc';

	EXECUTE IMMEDIATE 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE 1 = 1';

	RAISE NOTICE 'Executing V_LOAD_WKR_FORPLAN_RA: ''%''', V_LOAD_WKR_FORPLAN_RA;

	EXECUTE IMMEDIATE V_LOAD_WKR_FORPLAN_RA;

EXCEPTION 
	WHEN OTHERS THEN 
		L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
		L_ERR_MSG := SQLERRM; 
		RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
		RETURN L_ERR_CD;

END;

END_PROC;