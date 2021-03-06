SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_FACTOR_PROJECTION;

CREATE OR REPLACE PROCEDURE SP_LOAD_FACTOR_PROJECTION(BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64)) RETURNS INTEGER
language nzplsql
AS
BEGIN_PROC
 
DECLARE    
    P_REQUEST_ID                ALIAS FOR $1;
    P_SRC_SCHEMA                ALIAS FOR $2;
    P_SRC_TABLE                 ALIAS FOR $3;
    P_TRG_SCHEMA                ALIAS FOR $4; 
    P_TRG_TABLE                 ALIAS FOR $5;    
    L_ERR_CD                    CHAR(5);          
    L_ERR_MSG                   VARCHAR(32000);       
    V_REC                       RECORD;
    V_CLEAN_WRK_RD_FACTOR_PROJECTION   VARCHAR(64000);
    V_LOAD_WRK_RD_FACTOR_PROJECTION    VARCHAR(64000);
    
BEGIN
SET ISOLATION TO UR;
 
V_CLEAN_WRK_RD_FACTOR_PROJECTION := 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE REQUEST_ID =' || P_REQUEST_ID || ' ';

V_LOAD_WRK_RD_FACTOR_PROJECTION := 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
   CONTRACT_NUMBER              ,
   SECTION_NUMBER               ,
   UNDERWRITING_YEAR            ,
   CALCULATED_POSITION_ID       ,
   LEVEL_OF_ANALYSIS_ID         ,
   REPORTING_BASIS_ID           ,
   CLOSING_DATE                 ,
   ASSUMED_CONTRACT_NUMBER      ,
   ASSUMED_SECTION_NUMBER       ,
   PROJECTION_YEAR              ,
   PROJECTION_PERIOD            ,
   PERIOD_TYPE_ID               ,
   IS_PRESENT_VALUE             ,
   FACTOR                       ,
   REQUEST_ID                                
)
SELECT
    CAST(STG.CONTRACT_NUMBER AS CHAR(9))        AS CONTRACT_NUMBER ,
    CAST(STG.SECTION_NUMBER AS SMALLINT)        AS SECTION_NUMBER ,
    COALESCE(CASE WHEN STG.UWY = '''' THEN NULL ELSE CAST(STG.UWY as SMALLINT) END,0)  AS UNDERWRITING_YEAR,
    P.ID                                        AS CALCULATED_POSITION_ID,
    L.ID                                        AS LEVEL_OF_ANALYSIS_ID,
    RB.ID                                       AS REPORTING_BASIS_ID,
    REPLACE(TO_DATE(STG.CLOSING_DATE,''DD/MM/YYYY''), chr(13),'''') AS CLOSING_DATE,
    CASE WHEN STG.GROSS_ASSUMED_TREATY_NUMBER='''' THEN NULL ELSE CAST(STG.GROSS_ASSUMED_TREATY_NUMBER AS CHAR(9))END AS ASSUMED_CONTRACT_NUMBER,
    CASE WHEN STG.GROSS_ASSUMED_SECTION_NUMBER='''' THEN NULL ELSE CAST(STG.GROSS_ASSUMED_SECTION_NUMBER AS SMALLINT)END AS ASSUMED_SECTION_NUMBER, 
    CAST(STG.YEAR AS SMALLINT)                  AS PROJECTION_YEAR,
    CAST(STG.PERIOD AS SMALLINT)                AS PROJECTION_PERIOD,    
    PT.ID                                       AS PERIOD_TYPE_ID,
    (CASE CAST(STG.PV_FLAG AS CHAR(1)) WHEN ''Y'' THEN 1 WHEN ''N'' THEN 0 END) AS IS_PRESENT_VALUE,  
    CASE WHEN STG.RA_FACTOR = '''' THEN '''' ELSE CAST(STG.RA_FACTOR as DECFLOAT) END AS FACTOR,
    ' || P_REQUEST_ID || '                      AS REQUEST_ID    
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
INNER JOIN BI_<env>.POSITION P ON  UPPER(STG.CSM_CASHFLOW) = UPPER(P.CODE) AND DATE(P.VALID_TO) =  ''9999-12-31'' AND P.IS_CSM IS TRUE
INNER JOIN BI_<env>.LEVEL_OF_ANALYSIS L ON UPPER(STG.AOC_STEP) = UPPER(L.CODE) AND DATE(L.VALID_TO) =  ''9999-12-31'' 
INNER JOIN BI_<env>.REPORTING_BASIS RB ON UPPER(RB.CODE)=UPPER(STG.REPORTING_BASIS_CODE)
INNER JOIN BI_<env>.PERIOD_TYPE PT ON (UPPER(PT.CODE)=(CASE STG.PERIOD_ID WHEN 3 THEN ''Y'' WHEN 2 THEN  ''Q'' WHEN 1 THEN ''M'' END)) 
';

RAISE NOTICE 'Executing V_CLEAN_WRK_RD_FACTOR_PROJECTION: ''%''',V_CLEAN_WRK_RD_FACTOR_PROJECTION; 
EXECUTE IMMEDIATE V_CLEAN_WRK_RD_FACTOR_PROJECTION; 
RAISE NOTICE 'Executing V_LOAD_WRK_RD_FACTOR_PROJECTION: ''%''',V_LOAD_WRK_RD_FACTOR_PROJECTION; 
EXECUTE IMMEDIATE V_LOAD_WRK_RD_FACTOR_PROJECTION; 
EXECUTE IMMEDIATE 'COMMIT';

EXCEPTION WHEN OTHERS THEN
	L_ERR_CD := SUBSTR(SQLERRM,8,5); 
	L_ERR_MSG := SQLERRM;
	RAISE EXCEPTION '% Error while executing SQL statement',L_ERR_MSG;
	RETURN L_ERR_CD;

END;

END_PROC
;

