SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_WRK_UK_DWH_PROJECTION;

CREATE OR REPLACE PROCEDURE SP_LOAD_WRK_UK_DWH_PROJECTION (BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64)) RETURNS INTEGER 
LANGUAGE nzplsql AS 
BEGIN_PROC 

DECLARE 
    P_REQUEST_ID            ALIAS FOR $1;
    P_SRC_SCHEMA            ALIAS FOR $2;
    P_SRC_TABLE             ALIAS FOR $3;
    P_TRG_SCHEMA            ALIAS FOR $4;
    P_TRG_TABLE             ALIAS FOR $5;
    P_HIST_MODE             ALIAS FOR $6;
    L_ERR_CD                CHAR(5);
    L_ERR_MSG               VARCHAR(32000);
    V_REC                   RECORD;
    V_DELETE_QUERY          VARCHAR(32000);
    V_INSERT_QUERY          VARCHAR(32000);
    V_ASSUMED_TEMP_QUERY    VARCHAR(32000);
    V_RETRO_TEMP_QUERY      VARCHAR(32000);
    

BEGIN
SET ISOLATION TO UR;

V_ASSUMED_TEMP_QUERY := 'DROP TABLE '  || P_TRG_SCHEMA || '.TMP_RDWH_ASSUMED_UWY_UK IF EXISTS';
EXECUTE IMMEDIATE V_ASSUMED_TEMP_QUERY; 

V_ASSUMED_TEMP_QUERY := 'CREATE TABLE ' || P_TRG_SCHEMA || '.TMP_RDWH_ASSUMED_UWY_UK AS 
(
SELECT
    CTR_NF,
    SEC_NF,
    MAX(UWY_NF) ASSUMED
FROM(
    SELECT
                  SSD_CF,
                  ACCESB_CF,
                  CTR_NF,
                  SEC_NF,
                  SECSTS_CT,
                  ACCADMTYP_CT,
                  LIFPRDLINA_LL,
                  BUSUNIT_LM,
                  SUBMRK_LS,
                  UWY_NF,
                  ROW_NUMBER() OVER (PARTITION BY SSD_CF,ACCESB_CF,CTR_NF,SEC_NF,SECSTS_CT,ACCADMTYP_CT,LIFPRDLINA_LL,BUSUNIT_LM,SUBMRK_LS ORDER BY UWY_NF DESC) rank1
    FROM BI_<env>.TUWSEC
    WHERE LOB_CF IN (30,31)
        AND SECSTS_CT IN (14,16,19)
        AND SECACCSTS_CT NOT IN (9,8)
        AND BUSUNIT_LM IS NOT NULL
        AND DATE(END_D) = ''9999-12-31''
        AND DATE(SUPP_D)=''9999-12-31''
    )
WHERE rank1 = 1
GROUP BY CTR_NF,SEC_NF 
) 
WITH DATA DISTRIBUTE ON RANDOM
';
EXECUTE IMMEDIATE V_ASSUMED_TEMP_QUERY; 

V_RETRO_TEMP_QUERY := 'DROP TABLE '  || P_TRG_SCHEMA || '.TMP_RDWH_RETRO_UWY_UK  IF EXISTS';

EXECUTE IMMEDIATE V_RETRO_TEMP_QUERY; 
V_RETRO_TEMP_QUERY := 'CREATE TABLE ' || P_TRG_SCHEMA || '.TMP_RDWH_RETRO_UWY_UK AS 
(SELECT
    RETCTR_NF,
    RETSEC_NF,
    MAX(rty_nf) AS retro_value
FROM(
    SELECT
        TWT.RETCTR_NF,
        TWT.RETSEC_NF,
        TWT.RTY_NF,
        ROW_NUMBER() OVER (PARTITION BY TWT.SSD_CF,TWT.ESB_CF,TWT.RETCTR_NF,TWT.RETSEC_NF,DTT.RETCTRSTS_CT,DTT.RETACCTYP_CT,TWT.LIFPRDLINR_LL,TWT.MRKUNT_LS,TWT.SUBMRK_LS ORDER BY TWT.RTY_NF DESC) rank1
    FROM BI_<env>.TUWRETSEC TWT
    RIGHT JOIN BI_<env>.TRETCTR DTT ON (TWT.RETCTR_NF = DTT.RETCTR_NF AND TWT.RTY_NF = DTT.RTY_NF)
    WHERE TWT.LOB_CF IN (30,31)
        AND DTT.RETCTRSTS_CT IN (3,19)
        AND TWT.LIFPRDLINR_LL IS NOT NULL
        AND DATE(TWT.END_D) = ''9999-12-31''
        AND DATE(SUPP_D)=''9999-12-31'' 
    )
WHERE rank1 = 1
GROUP BY RETCTR_NF, RETSEC_NF
)
WITH DATA DISTRIBUTE ON RANDOM
';
EXECUTE IMMEDIATE V_RETRO_TEMP_QUERY; 

V_DELETE_QUERY := 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE REQUEST_ID =' || P_REQUEST_ID || ' ';

V_INSERT_QUERY :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(   CONTRACT_NUMBER,
    SECTION_NUMBER,
    ASSUMED_CONTRACT_NUMBER,
    ASSUMED_SECTION_NUMBER,
    UNDERWRITING_ORDER,
    ENDORSEMENT_NUMBER,
    UNDERWRITING_YEAR,
    AOC_STEP,
    SENSITIVITY_TYPE,
    SENSITIVITY_VALUE,
    POLICY_UW_YEAR,
    CLOSING_DATE,
    BUSINESS_MATURITY_ID,
    BUSINESS_MATURITY,
    REPORTING_BASIS_ID,
    BASIS,
    CREATED_BY,
    CREATED_DATE,
    POSITION,
    POSITION_ID,
    LEVEL_OF_ANALYSIS_ID,
    SCENARIO_TYPE_ID,
    SCENARIO_PARAMETER,
    CURRENCY_CODE,
    PROJECTION_YEAR,
    PROJECTION_MONTH,
    PERIOD_TYPE_ID,
    PROJECTION_DATE,
    AMOUNT,
    LINE_NUMBER,
    REQUEST_ID,
    SOURCE_REF_NAME,
    P_TIME,
    HISTORIZATION_MODE
)
SELECT 
    CONTRACT_NUMBER,
    SECTION_NUMBER,
    CASE WHEN LENGTH(ASSUMED_TREATY_NUMBER) > 0 THEN ASSUMED_TREATY_NUMBER END AS ASSUMED_TREATY_NUMBER,
    ASSUMED_SECTION_NUMBER,
    CASE WHEN ra.CTR_NF IS NOT NULL THEN 1 END AS UNDERWRITING_ORDER,
    CASE WHEN ra.CTR_NF IS NOT NULL THEN 0 END AS ENDORSEMENT_NUMBER,
    CASE WHEN RETRO_VALUE IS NULL THEN ASSUMED ELSE RETRO_VALUE END AS UNDERWRITING_YEAR,
    WRK.AOC_STEP,
    WRK.SENSITIVITY_TYPE,
    WRK.SENSITIVITY_VALUE,
    WRK.POLICY_UWY,
    WRK.CLOSING_DATE,
    CASE WHEN UPPER(WRK.BUSINESS_MATURITY) IN (UPPER(DM.Name)) THEN DM.ID ELSE 9999 END AS BUSINESS_MATURITY_ID ,
    WRK.BUSINESS_MATURITY,
    CASE WHEN UPPER(WRK.REPORTING_BASIS) IN (UPPER(RB.Name)) THEN RB.ID ELSE 9999 END AS REPORTING_BASIS_ID,
    WRK.REPORTING_BASIS,
    WRK.CREATED_BY,
    WRK.CREATED_DATE,
    WRK.POSITION,
    WRK.POSITION_ID,
    CASE WHEN UPPER(WRK.AOC_STEP) IN (UPPER(lvl.Name)) THEN lvl.ID ELSE 9999 END AS LEVEL_OF_ANALYSIS_ID,
    CASE WHEN UPPER(WRK.SENSITIVITY_TYPE) IN (UPPER(scn.Name)) THEN scn.ID ELSE 9999 END AS SCENARIO_TYPE_ID,
    SENSITIVITY_VALUE AS SCENARIO_PARAMETER,
    CASE WHEN WRK.POSITION IN(''REN_EXP_ATTRIBUTABLE'',''REN_EXP'') THEN EXPENSE_CURRENCY ELSE CURRENCY END AS CURRENCY_CODE,
    YEAR(TIME) AS PROJECTION_YEAR,
    MONTH(TIME) AS PROJECTION_MONTH,
    3 AS PERIOD_TYPE_ID,
    LAST_DAY(YEAR(TIME) || ''-'' || MONTH(TIME) || ''-'' || ''01'') AS PROJECTION_DATE,
    AMOUNT,
    LINE_NUMBER,
    REQUEST_ID,
    ''UK PROPHET'' AS SOURCE_REF_NAME,
    TIME,
    ''' || P_HIST_MODE || ''' AS HISTORIZATION_MODE
FROM (SELECT
        WRK.JOIN_KEY,
        WRK.PRODUCT,
        WRK.ASSUMED_TREATY_NUMBER,
        WRK.ASSUMED_SECTION_NUMBER,
        WRK.CONTRACT_NUMBER,
        WRK.SECTION_NUMBER,
        WRK.RUN_NUMBER,
        WRK.AOC_STEP,
        WRK.SENSITIVITY_TYPE,
        WRK.SENSITIVITY_VALUE,
        WRK.SPCODE,
        WRK.POLICY_UWY,
        WRK.BUSINESS_MATURITY,
        WRK.NEW_BUSINESS,
        WRK.CLOSING_DATE,
        WRK.PERIOD,
        WRK.TIME,
        WRK.REPORTING_BASIS,
        WRK.CURRENCY,
        WRK.EXPENSE_CURRENCY,
		POS.NAME AS POSITION,
		POS.POSITION_ID,
        WRK.AMOUNT,
        WRK.CREATED_BY,
        WRK.CREATED_DATE,
        WRK.LINE_NUMBER,
        WRK.REQUEST_ID
    FROM (
        WITH T(POSITION_CODE) as 
        (VALUES 
            (''PREM_INC''),
            (''OPT_PREM_INC''),
            (''EXT_PREM_INC''),
            (''CLAIMS_INCURRED''),
            (''CLAIMS_FROM_IBNP''),
            (''PROFIT_COMM''),
            (''IC_CLAWED_L''),
            (''REN_EXP_ATTRIBUTABLE''),
            (''IBNR_RES_IF''),
            (''OS_LOSS_RES''),
            (''SUM_ASSD_IF''),
            (''INIT_COMM''),
            (''REN_EXP_NONATTRIBUTABLE''),
            (''REN_COMM''),
            (''TAX''),
            (''NO_POLS_IF''),
            (''UPR_RES_IF''),
            (''MATH_RES_IF''),
            (''EXT_PREM_RES''),
            (''CONT_RES_IF''),
            (''PS_RESERVE''),
            (''OPT_RES_IF''),
            (''WOP_RES_IF''),
            (''CIP_RES_IF''),
            (''DAC_IF''),
            (''EXTPRMDAC_IF''),
            (''RES_NONDEPINT''),
            (''DISC_PROF_B''),
            (''PROFIT''),
            (''ILS_PREM''),
            (''ANUITY_OUTGO''),
            (''ILS_CLAIMS''),
            (''ILS_PROFIT_COMM''),
            (''REN_EXP''),
            (''ILS_MAT_PROFIT_COMM''),
            (''COVERAGE_UNITS'')
        )
        SELECT
            STG.JOIN_KEY,
            STG.PRODUCT,
            STG.ASSUMED_TREATY_NUMBER,
            STG.ASSUMED_SECTION_NUMBER,
            STG.CONTRACT_NUMBER,
            STG.SECTION_NUMBER,
            STG.RUN_NUMBER,
            STG.AOC_STEP,
            STG.SENSITIVITY_TYPE,
            STG.SENSITIVITY_VALUE,
            STG.SPCODE,
            STG.POLICY_UWY,
            STG.BUSINESS_MATURITY,
            STG.NEW_BUSINESS,
            STG.CLOSING_DATE,
            STG.PERIOD,
            STG.TIME,
            STG.REPORTING_BASIS,
            STG.CURRENCY,
            STG.EXPENSE_CURRENCY,
            T.POSITION_CODE AS POSITION,
            CASE T.POSITION_CODE
                WHEN ''PREM_INC''                     THEN STG.PREM_INC                     
                WHEN ''OPT_PREM_INC''                 THEN STG.OPT_PREM_INC                 
                WHEN ''EXT_PREM_INC''                 THEN STG.EXT_PREM_INC                 
                WHEN ''CLAIMS_INCURRED''              THEN STG.CLAIMS_INCURRED              
                WHEN ''CLAIMS_FROM_IBNP''             THEN STG.CLAIMS_FROM_IBNP             
                WHEN ''PROFIT_COMM''                  THEN STG.PROFIT_COMM                  
                WHEN ''IC_CLAWED_L''                  THEN STG.IC_CLAWED_L                  
                WHEN ''REN_EXP_ATTRIBUTABLE''         THEN STG.REN_EXP_ATTRIBUTABLE         
                WHEN ''IBNR_RES_IF''                  THEN STG.IBNR_RES_IF                  
                WHEN ''OS_LOSS_RES''                  THEN STG.OS_LOSS_RES                  
                WHEN ''SUM_ASSD_IF''                  THEN STG.SUM_ASSD_IF                  
                WHEN ''INIT_COMM''                    THEN STG.INIT_COMM                    
                WHEN ''REN_EXP_NONATTRIBUTABLE''      THEN STG.REN_EXP_NONATTRIBUTABLE      
                WHEN ''REN_COMM''                     THEN STG.REN_COMM                     
                WHEN ''TAX''                          THEN STG.TAX                          
                WHEN ''NO_POLS_IF''                   THEN STG.NO_POLS_IF                   
                WHEN ''UPR_RES_IF''                   THEN STG.UPR_RES_IF                   
                WHEN ''MATH_RES_IF''                  THEN STG.MATH_RES_IF                  
                WHEN ''EXT_PREM_RES''                 THEN STG.EXT_PREM_RES                 
                WHEN ''CONT_RES_IF''                  THEN STG.CONT_RES_IF                  
                WHEN ''PS_RESERVE''                   THEN STG.PS_RESERVE                   
                WHEN ''OPT_RES_IF''                   THEN STG.OPT_RES_IF                   
                WHEN ''WOP_RES_IF''                   THEN STG.WOP_RES_IF                   
                WHEN ''CIP_RES_IF''                   THEN STG.CIP_RES_IF                   
                WHEN ''DAC_IF''                       THEN STG.DAC_IF                       
                WHEN ''EXTPRMDAC_IF''                 THEN STG.EXTPRMDAC_IF                 
                WHEN ''RES_NONDEPINT''                THEN STG.RES_NONDEPINT                
                WHEN ''DISC_PROF_B''                  THEN STG.DISC_PROF_B                  
                WHEN ''PROFIT''                       THEN STG.PROFIT                       
                WHEN ''ILS_PREM''                     THEN STG.ILS_PREM                     
                WHEN ''ANUITY_OUTGO''                 THEN STG.ANUITY_OUTGO                 
                WHEN ''ILS_CLAIMS''                   THEN STG.ILS_CLAIMS                   
                WHEN ''ILS_PROFIT_COMM''              THEN STG.ILS_PROFIT_COMM              
                WHEN ''REN_EXP''                      THEN STG.REN_EXP                      
                WHEN ''ILS_MAT_PROFIT_COMM''          THEN STG.ILS_MAT_PROFIT_COMM          
                WHEN ''COVERAGE_UNITS''               THEN STG.COVERAGE_UNITS               
            ELSE NULL END AS  AMOUNT,
            STG.CREATED_BY,
            STG.CREATED_DATE,
            STG.LINE_NUMBER,
            STG.REQUEST_ID
        FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
        INNER JOIN T ON NVL(MOD(LENGTH(T.POSITION_CODE), 1), 0) = NVL(MOD(LENGTH(STG.NEW_BUSINESS), 1), 0)
        ) WRK
        INNER JOIN (
            SELECT
				POS.NAME ,
                POS.CODE ,
                PM.POSITION_ID
            FROM BI_<env>.POSITION pos
            INNER JOIN BI_<env>.POSITION_MAPPING PM ON POS.ID = PM.MAPPING_POSITION_ID  AND DATE(POS.VALID_TO) = ''9999-12-31'' AND DATE(pm.VALID_TO) = ''9999-12-31''
            INNER JOIN BI_<env>.POSITION_TYPE PT ON PT.ID=POS.POSITION_TYPE_ID AND PT.CODE = ''UK''   
            ) POS ON (UCASE(POS.CODE) = UCASE(WRK.POSITION))
    ) WRK
LEFT OUTER JOIN ' || P_TRG_SCHEMA || '.TMP_RDWH_ASSUMED_UWY_UK DTW ON (WRK.CONTRACT_NUMBER = DTW.CTR_NF AND WRK.SECTION_NUMBER = DTW.SEC_NF)
LEFT OUTER JOIN ' || P_TRG_SCHEMA || '.TMP_RDWH_RETRO_UWY_UK RET ON (WRK.CONTRACT_NUMBER = RET.RETCTR_NF AND WRK.SECTION_NUMBER = RET.RETSEC_NF)
LEFT OUTER JOIN ( SELECT
                    CTR_NF
            FROM BI_<env>.TCONTR
            GROUP BY CTR_NF
    ) RA ON ( RA.CTR_NF = CONTRACT_NUMBER )
LEFT OUTER JOIN BI_<env>.SCENARIO_TYPE SCN ON (UCASE(SCN.NAME) = UCASE(WRK.SENSITIVITY_TYPE) AND DATE(SCN.VALID_TO) = ''9999-12-31'')
LEFT OUTER JOIN BI_<env>.LEVEL_OF_ANALYSIS LVL ON ( UCASE(LVL.NAME) = UCASE(WRK.AOC_STEP) AND DATE(LVL.VALID_TO) = ''9999-12-31'')
LEFT OUTER JOIN BI_<env>.BUSINESS_MATURITY DM ON (UCASE(DM.NAME) = UCASE(BUSINESS_MATURITY))
LEFT OUTER JOIN BI_<env>.REPORTING_BASIS RB ON (UCASE(RB.NAME) = UCASE(REPORTING_BASIS))
WHERE WRK.REQUEST_ID = ' || P_REQUEST_ID || '
';

RAISE NOTICE 'Executing V_DELETE_QUERY: ''%''',V_DELETE_QUERY; 
EXECUTE IMMEDIATE V_DELETE_QUERY; 

RAISE NOTICE 'Executing V_INSERT_QUERY: ''%''',V_INSERT_QUERY;
EXECUTE IMMEDIATE V_INSERT_QUERY;

V_ASSUMED_TEMP_QUERY := 'DROP TABLE '  || P_TRG_SCHEMA || '.TMP_RDWH_ASSUMED_UWY_UK IF EXISTS';
EXECUTE IMMEDIATE V_ASSUMED_TEMP_QUERY; 

V_RETRO_TEMP_QUERY := 'DROP TABLE '  || P_TRG_SCHEMA || '.TMP_RDWH_RETRO_UWY_UK IF EXISTS';
EXECUTE IMMEDIATE V_RETRO_TEMP_QUERY; 


EXCEPTION WHEN OTHERS THEN L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;