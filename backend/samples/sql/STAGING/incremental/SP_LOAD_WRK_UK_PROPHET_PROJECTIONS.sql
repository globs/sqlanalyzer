SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_WRK_UK_PROPHET_PROJECTIONS;

CREATE OR REPLACE PROCEDURE SP_LOAD_WRK_UK_PROPHET_PROJECTIONS (BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64)) RETURNS INTEGER 
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

V_INSERT_QUERY :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (   
    JOIN_KEY                     ,
    PRODUCT                      ,
    CONTRACT_NUMBER              ,
    SECTION_NUMBER               ,
    ASSUMED_TREATY_NUMBER        ,
    ASSUMED_SECTION_NUMBER       ,
    RUN_NUMBER                   ,
    AOC_STEP                     ,
    SENSITIVITY_TYPE             ,
    SENSITIVITY_VALUE            ,
    SPCODE                       ,
    POLICY_UWY                   ,
    BUSINESS_MATURITY            ,
    NEW_BUSINESS                 ,
    CLOSING_DATE                 ,
    PERIOD                       ,
    TIME                         ,
    REPORTING_BASIS              ,
    CURRENCY                     ,
    EXPENSE_CURRENCY             ,
    PREM_INC                     ,
    OPT_PREM_INC                 ,
    EXT_PREM_INC                 ,
    CLAIMS_INCURRED              ,
    CLAIMS_FROM_IBNP             ,
    PROFIT_COMM                  ,
    IC_CLAWED_L                  ,
    REN_EXP_ATTRIBUTABLE         ,
    IBNR_RES_IF                  ,
    OS_LOSS_RES                  ,
    SUM_ASSD_IF                  ,
    INIT_COMM                    ,
    REN_EXP_NONATTRIBUTABLE      ,
    REN_COMM                     ,
    TAX                          ,
    NO_POLS_IF                   ,
    UPR_RES_IF                   ,
    MATH_RES_IF                  ,
    EXT_PREM_RES                 ,
    CONT_RES_IF                  ,
    PS_RESERVE                   ,
    OPT_RES_IF                   ,
    WOP_RES_IF                   ,
    CIP_RES_IF                   ,
    DAC_IF                       ,
    EXTPRMDAC_IF                 ,
    RES_NONDEPINT                ,
    DISC_PROF_B                  ,
    PROFIT                       ,
    ILS_PREM                     ,
    ANUITY_OUTGO                 ,
    ILS_CLAIMS                   ,
    ILS_PROFIT_COMM              ,
    REN_EXP                      ,
    ILS_MAT_PROFIT_COMM          ,
    COVERAGE_UNITS               ,
    CREATED_BY                   ,
    CREATED_DATE                 ,
    LINE_NUMBER                  ,
    REQUEST_ID
)
SELECT 
    TRIM(JOINKEY), 
    TRIM(PRODUCT), 
    TRIM(OMEGATREATYNUMBER)           AS CONTRACT_NUMBER ,
    CAST(OMEGASECTION AS INT)   AS SECTION_NUMBER,
    CASE WHEN TRIM(GROSS_ASSUMED_OMEGA_TREATY_NUMBER)='''' THEN NULL ELSE GROSS_ASSUMED_OMEGA_TREATY_NUMBER END AS ASSUMED_TREATY_NUMBER ,
    CAST(CASE WHEN TRIM(GROSS_ASSUMED_OMEGA_SECTION)<>'''' THEN GROSS_ASSUMED_OMEGA_SECTION END  AS INT) AS ASSUMED_SECTION_NUMBER,
    CAST(RUNNUMBER AS INT)      AS RUN_NUMBER,
    TRIM(AOCSTEP)                     AS AOC_STEP,
    TRIM(SENSITIVITY_TYPE)            AS SENSITIVITY_TYPE,
    CAST(SENSITIVITY_VALUE AS DECIMAL(10,6)) AS SENSITIVITY_VALUE,
    CAST(SPCODE AS INT)         AS SPCODE,
    CAST(CASE WHEN TRIM(POLICY_UWY)<>'''' THEN POLICY_UWY END  AS INT) POLICY_UWY,
    TRIM(BUSINESS_MATURITY),
    CAST(NEW_BUSINESS AS INT)   AS NEW_BUSINESS,
    TO_DATE(CLOSING_DATE,''DD/MM/YYYY'') AS CLOSING_DATE,
    CAST(PERIOD AS INT)         AS PERIOD,
    TO_DATE(TIME,''DD/MM/YYYY'') AS TIME,
    TRIM(REPORTING_BASIS),
    TRIM(CURRENCY),
    TRIM(EXPENSECURRENCY),
    CAST (PREM_INC AS DECFLOAT), 
    CAST (OPT_PREM_INC AS DECFLOAT), 
    CAST (EXT_PREM_INC AS DECFLOAT), 
    CAST (CLAIMS_INCURRED AS DECFLOAT), 
    CAST (CLAIMS_FROM_IBNP AS DECFLOAT), 
    CAST (PROFIT_COMM AS DECFLOAT), 
    CAST (IC_CLAWED_L AS DECFLOAT), 
    CAST (REN_EXP_ATTRIBUTABLE AS DECFLOAT), 
    CAST (IBNR_RES_IF AS DECFLOAT), 
    CAST (OS_LOSS_RES AS DECFLOAT), 
    CAST (SUM_ASSD_IF AS DECFLOAT), 
    CAST (INIT_COMM AS DECFLOAT), 
    CAST (REN_EXP_NONATTRIBUTABLE AS DECFLOAT), 
    CAST (REN_COMM AS DECFLOAT), 
    CAST (TAX AS DECFLOAT), 
    CAST (NO_POLS_IF AS DECFLOAT), 
    CAST (UPR_RES_IF AS DECFLOAT), 
    CAST (MATH_RES_IF AS DECFLOAT), 
    CAST (EXT_PREM_RES AS DECFLOAT), 
    CAST (CONT_RES_IF AS DECFLOAT), 
    CAST (PS_RESERVE AS DECFLOAT), 
    CAST (OPT_RES_IF AS DECFLOAT), 
    CAST (WOP_RES_IF AS DECFLOAT), 
    CAST (CIP_RES_IF AS DECFLOAT), 
    CAST (DAC_IF AS DECFLOAT), 
    CAST (EXTPRMDAC_IF AS DECFLOAT), 
    CAST (RES_NONDEPINT AS DECFLOAT), 
    CAST (DISC_PROF_B AS DECFLOAT), 
    CAST (PROFIT AS DECFLOAT), 
    CAST (ILS_PREM AS DECFLOAT), 
    CAST (ANUITY_OUTGO AS DECFLOAT), 
    CAST (ILS_CLAIMS AS DECFLOAT), 
    CAST (ILS_PROFIT_COMM AS DECFLOAT), 
    CAST (REN_EXP AS DECFLOAT), 
    CAST (ILS_MAT_PROFIT_COMM AS DECFLOAT), 
    CAST (COVERAGE_UNITS AS DECFLOAT), 
    ''UK PROPHET''          AS CREATED_BY,
    CURRENT_TIMESTAMP       AS CREATED_DATE,
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