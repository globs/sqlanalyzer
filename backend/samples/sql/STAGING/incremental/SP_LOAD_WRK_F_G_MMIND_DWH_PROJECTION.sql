SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_WRK_F_G_MMIND_DWH_PROJECTION;

CREATE OR REPLACE PROCEDURE SP_LOAD_WRK_F_G_MMIND_DWH_PROJECTION (BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64)) RETURNS INTEGER 
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

V_ASSUMED_TEMP_QUERY := 'DROP TABLE '  || P_TRG_SCHEMA || '.TMP_RDWH_ASSUMED_UWY_FRGE IF EXISTS';
EXECUTE IMMEDIATE V_ASSUMED_TEMP_QUERY; 

V_ASSUMED_TEMP_QUERY := '
CREATE TABLE ' || P_TRG_SCHEMA || '.TMP_RDWH_ASSUMED_UWY_FRGE AS 
(
SELECT
    CTR_NF,
    SEC_NF,
    CAST(MAX(UWY_NF) AS VARCHAR) ASSUMED
FROM (SELECT
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

V_RETRO_TEMP_QUERY := 'DROP TABLE '  || P_TRG_SCHEMA || '.TMP_RDWH_RETRO_UWY_FRGE IF EXISTS';
EXECUTE IMMEDIATE V_RETRO_TEMP_QUERY; 

V_RETRO_TEMP_QUERY := '
CREATE TABLE ' || P_TRG_SCHEMA || '.TMP_RDWH_RETRO_UWY_FRGE AS 
(
SELECT
    RETCTR_NF,
    RETSEC_NF,
    CAST(MAX(rty_nf) AS VARCHAR) retro_value
FROM (SELECT
        TWT.RETCTR_NF,
        TWT.RETSEC_NF,
        TWT.RTY_NF,
        ROW_NUMBER() OVER (PARTITION BY TWT.SSD_CF,TWT.ESB_CF,TWT.RETCTR_NF,TWT.RETSEC_NF,DTT.RETCTRSTS_CT,DTT.RETACCTYP_CT,TWT.LIFPRDLINR_LL,TWT.MRKUNT_LS,TWT.SUBMRK_LS ORDER BY TWT.RTY_NF DESC) rank1
    FROM BI_<env>.TUWRETSEC TWT
    RIGHT JOIN BI_<env>.TRETCTR DTT ON (TWT.RETCTR_NF = DTT.RETCTR_NF AND TWT.RTY_NF = DTT.RTY_NF)
    WHERE  TWT.LOB_CF IN (30,31)
        AND DTT.RETCTRSTS_CT IN (3,19)
        AND TWT.LIFPRDLINR_LL IS NOT NULL
        AND DATE(TWT.END_D) = ''9999-12-31''
        AND DATE(SUPP_D)=''9999-12-31''
    )
WHERE rank1 = 1
GROUP BY RETCTR_NF,RETSEC_NF
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
	AOC_STEP,
	LEVEL_OF_ANALYSIS_ID,
	SENSITIVITY_TYPE,
	SCENARIO_TYPE_ID,
	SENSITIVITY_VALUE,
	SCENARIO_PARAMETER,
	POLICY_UW_YEAR,
	UNDERWRITING_YEAR,
	BUSINESS_MATURITY,
	BUSINESS_MATURITY_ID,
	POSITION,
	POSITION_ID,
	CURRENCY_CODE,
	BASIS,
	REPORTING_BASIS_ID,
	CLOSING_DATE,
	HISTORIZATION_MODE,
	PROJECTION_MONTH,
	PROJECTION_YEAR,
	PROJECTION_DATE,
	PERIOD_TYPE_ID,
	PERIOD_DESCRIPTION,
	AMOUNT,
	CREATED_BY,
	CREATED_DATE,
	LINE_NUMBER,
	REQUEST_ID,
	SOURCE_REF_NAME
)
SELECT
    SRC.CONTRACT_NUMBER,
    SRC.SECTION_NUMBER, 
    CASE WHEN LENGTH(SRC.ASSUMED_CONTRACT_NUMBER) > 0 THEN SRC.ASSUMED_CONTRACT_NUMBER END AS ASSUMED_CONTRACT_NUMBER, 
    SRC.ASSUMED_SECTION_NUMBER,
    CASE WHEN ra.CTR_NF IS NOT NULL THEN ''1'' ELSE NULL END AS UNDERWRITTING_ORDER,
    CASE WHEN ra.CTR_NF IS NOT NULL THEN ''0'' ELSE NULL END AS ENDORSEMENT_NUMBER,
    SRC.AOC_STEP,
    CASE WHEN LVL.ID IS NOT NULL THEN LVL.ID ELSE 9999 END AS LEVEL_OF_ANALYSIS_ID,
    SRC.SENSITIVITY_TYPE,
    CASE WHEN SCN.ID IS NOT NULL THEN SCN.ID ELSE 9999 END AS SCENARIO_TYPE_ID,
    SRC.SENSITIVITY_VALUE,
    SRC.SENSITIVITY_VALUE AS SCENARIO_PARAMETER,
    CASE WHEN LENGTH(SRC.POLICY_UWY) <> 0 THEN SRC.POLICY_UWY ELSE NULL END AS POLICY_UWY,
    CASE WHEN RET.RETRO_VALUE IS NOT NULL THEN RET.RETRO_VALUE ELSE DTW.ASSUMED END UNDERWRITING_YEAR,
    SRC.BUSINESS_MATURITY,
    CASE WHEN DM.ID IS NOT NULL THEN DM.ID ELSE 9999 END AS BUSINESS_MATURITY_ID,
    SRC.POSITION,
    CASE WHEN PN.ID IS NOT NULL THEN PN.ID ELSE 9999 END AS POSITION_ID,
    UCASE(SRC.CURRENCY) AS CURRENCY_CODE,
    SRC.REPORTING_BASIS_TYPE AS BASIS,
    CASE WHEN RB.ID IS NOT NULL THEN RB.ID ELSE 9999 END AS REPORTING_BASIS_ID,
    SRC.CLOSING_DATE,
    ''' || P_HIST_MODE || '''  AS HISTORIZATION_MODE,
    SRC.PROJECTION_MONTH,
    SRC.PROJECTION_YEAR,
    LAST_DAY(SRC.PROJECTION_YEAR || ''-'' || SRC.PROJECTION_MONTH || ''-'' || ''01'') AS PROJECTION_DATE,
    CASE
        WHEN SRC.PERIOD_DESCRIPTION LIKE (''M%'') THEN 3
        WHEN SRC.PERIOD_DESCRIPTION LIKE (''Q%'') THEN 2
        WHEN SRC.PERIOD_DESCRIPTION LIKE (''Yearly'') THEN 1
    END AS PERIOD_TYPE_ID,
    SRC.PERIOD_DESCRIPTION,
    SRC.PROJECTION_AMOUNT AS AMOUNT,
    SRC.CREATED_BY,
    SRC.CREATED_DATE,
    SRC.LINE_NUMBER,
    SRC.REQUEST_ID,
    UCASE(SRC.CREATED_BY) AS SOURCE_REF_NAME
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' SRC
LEFT OUTER JOIN ' || P_TRG_SCHEMA || '.TMP_RDWH_ASSUMED_UWY_FRGE DTW ON (src.CONTRACT_NUMBER = DTW.CTR_NF AND CAST(src.SECTION_NUMBER AS INTEGER) = DTW.SEC_NF)
LEFT OUTER JOIN ' || P_TRG_SCHEMA || '.TMP_RDWH_RETRO_UWY_FRGE RET ON (src.CONTRACT_NUMBER = RET.RETCTR_NF AND CAST(src.SECTION_NUMBER AS INTEGER) = RET.RETSEC_NF)
LEFT OUTER JOIN (SELECT
        CTR_NF
    FROM
    BI_<env>.TCONTR
    GROUP BY CTR_NF
    ) ra ON( ra.CTR_NF = CONTRACT_NUMBER )
LEFT OUTER JOIN (SELECT
        POS.NAME,
        POS.ID,
        PTY.CODE
    FROM BI_<env>.POSITION POS
    JOIN BI_<env>.POSITION_TYPE PTY ON POS.POSITION_TYPE_ID = PTY.ID
    WHERE PTY.CODE = ''RDWH'' AND DATE(POS.VALID_TO) = ''9999-12-31''
    ) PN ON UCASE(PN.NAME) = UCASE(src.POSITION)
LEFT OUTER JOIN BI_<env>.BUSINESS_MATURITY DM ON (UCASE(DM.NAME) = UCASE(BUSINESS_MATURITY))
LEFT OUTER JOIN BI_<env>.REPORTING_BASIS RB ON (UCASE(RB.NAME) = UCASE(REPORTING_BASIS_TYPE))
LEFT OUTER JOIN (SELECT
        NAME,
        ID
    FROM BI_<env>.LEVEL_OF_ANALYSIS
    WHERE DATE(VALID_TO) = ''9999-12-31''
    ) LVL ON UCASE(LVL.NAME) = UCASE(src.AOC_STEP)
LEFT OUTER JOIN (SELECT
        NAME,
        ID
    FROM BI_<env>.SCENARIO_TYPE
    WHERE DATE(VALID_TO) = ''9999-12-31''
    ) SCN ON UCASE(SCN.NAME) = UCASE(src.SENSITIVITY_TYPE)
WHERE SRC.PROJECTION_AMOUNT IS NOT NULL AND SRC.REQUEST_ID = ' || P_REQUEST_ID || '
';

RAISE NOTICE 'Executing V_DELETE_QUERY: ''%''',V_DELETE_QUERY; 
EXECUTE IMMEDIATE V_DELETE_QUERY; 

RAISE NOTICE 'Executing V_INSERT_QUERY: ''%''',V_INSERT_QUERY;
EXECUTE IMMEDIATE V_INSERT_QUERY;

V_ASSUMED_TEMP_QUERY := 'DROP TABLE '  || P_TRG_SCHEMA || '.TMP_RDWH_ASSUMED_UWY_FRGE IF EXISTS';
EXECUTE IMMEDIATE V_ASSUMED_TEMP_QUERY; 

V_RETRO_TEMP_QUERY := 'DROP TABLE '  || P_TRG_SCHEMA || '.TMP_RDWH_RETRO_UWY_FRGE IF EXISTS';
EXECUTE IMMEDIATE V_RETRO_TEMP_QUERY; 

EXCEPTION WHEN OTHERS THEN L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;