SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_WRK_F_G_MMIND_PROJECTIONS;

CREATE OR REPLACE PROCEDURE SP_LOAD_WRK_F_G_MMIND_PROJECTIONS (BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64)) RETURNS INTEGER 
LANGUAGE nzplsql AS 
BEGIN_PROC 

DECLARE 
    P_REQUEST_ID            ALIAS FOR $1;
    P_SRC_SCHEMA            ALIAS FOR $2;
    P_SRC_TABLE             ALIAS FOR $3;
    P_TRG_SCHEMA            ALIAS FOR $4;
    P_TRG_TABLE             ALIAS FOR $5;
    L_ERR_CD                CHAR(5);
    L_ERR_MSG               VARCHAR(32000);
    V_REC                   RECORD;
    V_DELETE_QUERY          VARCHAR(32000);
    V_INSERT_QUERY          VARCHAR(32000);

BEGIN
SET ISOLATION TO UR;

V_DELETE_QUERY := 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE REQUEST_ID =' || P_REQUEST_ID || ' ';

V_INSERT_QUERY :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(   CLOSING_DATE,
	MODELLING_REGION,
	PRODUCTIVE_NONPRODUCTIVE,
	CONTRACT_NUMBER,
	SECTION_NUMBER,
	ASSUMED_CONTRACT_NUMBER,
	ASSUMED_SECTION_NUMBER,
	AOC_STEP,
	SENSITIVITY_TYPE,
	SENSITIVITY_VALUE,
	POLICY_UWY,
	BUSINESS_MATURITY,
	POSITION,
	CURRENCY,
	REPORTING_BASIS_TYPE,
	PERIOD_DESCRIPTION,
	PROJECTION_MONTH,
	PROJECTION_YEAR,
	PROJECTION_AMOUNT,
	CREATED_BY,
	CREATED_DATE,
	LINE_NUMBER,
	REQUEST_ID
)
SELECT
	TO_DATE(CLOSING_DATE, ''DD/MM/YYYY'') AS CLOSING_DATE,
	MODELLING_REGION,
	PRODUCTIVE_NONPRODUCTIVE,
	OMEGA_TREATY_NUMBER AS CONTRACT_NUMBER,
	OMEGA_SECTION AS SECTION_NUMBER,
	CASE WHEN LENGTH(GROSS_ASSUMED_OMEGA_TREATY_NUMBER) > 0 THEN GROSS_ASSUMED_OMEGA_TREATY_NUMBER END  AS ASSUMED_CONTRACT_NUMBER,
	CASE WHEN LENGTH(GROSS_ASSUMED_OMEGA_SECTION) > 0  THEN GROSS_ASSUMED_OMEGA_SECTION END AS ASSUMED_SECTION_NUMBER,
	AOC_STEP,
	SENSITIVITY_TYPE,
	CAST(SENSITIVITY_VALUE AS DECIMAL(10,6)) SENSITIVITY_VALUE,
	CASE WHEN POLICY_UWY <> '''' THEN POLICY_UWY END AS POLICY_UWY,
	BUSINESS_MATURITY,
	POSITION,
	CURRENCY,
	REPORTING_BASIS_TYPE,
	PERIOD_DESCRIPTION,
	CASE
		WHEN PERIOD_DESCRIPTION = ''Q1'' THEN 3
		WHEN PERIOD_DESCRIPTION = ''Q2'' THEN 6
		WHEN PERIOD_DESCRIPTION = ''Q3'' THEN 9
		WHEN PERIOD_DESCRIPTION LIKE (''M%'') THEN SUBSTRING(PERIOD_DESCRIPTION, 2)
	ELSE 12 END AS PROJECTION_MONTH,
	UCASE(SUBSTRING(PERIOD, 1, 4)) AS PROJECTION_YEAR,
	CASE WHEN LENGTH(PROJECTION_AMOUNT) <> 0 OR PROJECTION_AMOUNT <> '''' THEN CAST(PROJECTION_AMOUNT AS DECFLOAT) END AS PROJECTION_AMOUNT,
    (MODELLING_REGION || ''_MMind'') AS CREATED_BY,
	CURRENT_TIMESTAMP AS CREATED_DATE,
	LINE_NUMBER,
	' || P_REQUEST_ID || '
FROM  ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
';

RAISE NOTICE 'Executing V_DELETE_QUERY: ''%''',V_DELETE_QUERY; 
EXECUTE IMMEDIATE V_DELETE_QUERY; 

RAISE NOTICE 'Executing V_INSERT_QUERY: ''%''',V_INSERT_QUERY;
EXECUTE IMMEDIATE V_INSERT_QUERY;

EXCEPTION WHEN OTHERS THEN L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;