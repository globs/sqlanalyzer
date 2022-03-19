SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_CHECK_RISK_ADJUSTMENT_FACTOR_PARAM;

CREATE OR REPLACE PROCEDURE SP_LOAD_CHECK_RISK_ADJUSTMENT_FACTOR_PARAM 
( 
BIGINT,
CHARACTER VARYING(50),
CHARACTER VARYING(50),
CHARACTER VARYING(50),
CHARACTER VARYING(50) 
) 
RETURNS INTEGER
LANGUAGE NZPLSQL 
AS BEGIN_PROC

DECLARE 
    P_REQUEST_ID    ALIAS FOR $1;
    P_SRC_SCHEMA    ALIAS FOR $2;
    P_SRC_TABLE     ALIAS FOR $3;
    P_TRG_SCHEMA    ALIAS FOR $4;
    P_TRG_TABLE     ALIAS FOR $5;
    V_REQ_ID        VARCHAR(20);
    V_WRK_TBL       VARCHAR(1000);
    L_ERR_CD        CHAR(5);
    L_ERR_MSG       VARCHAR(32000);

BEGIN
SET ISOLATION TO UR;
V_REQ_ID := UPPER(TRIM(P_REQUEST_ID));
V_WRK_TBL := 'CTRL1_WRK_' || P_REQUEST_ID;

EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TBL || ' IF EXISTS';
EXECUTE IMMEDIATE 'DECLARE GLOBAL TEMPORARY TABLE SESSION.' || V_WRK_TBL || ' AS (
SELECT 
    LINE_NUMBER,
    SUBSIDIARY_CODE,
    LEDGER_CODE,
    REGION_CODE,
    TREATY_LIFE_CHARACTERISTIC_CODE,
    FINANCING_TYPE_CODE,
    IAS39_CODE,
    USGAAP_CODE,
    COINSURANCE_CODE,
    TYPE_OF_BUSINESS_CODE,
    GUARANTEE_CODE,
    POLICY_TYPE_CODE,
    NATURE_CODE,
    CLIENT_ID, 
    BUSINESSMATURITY_CODE,
    MARKET_UNIT_CODE,
    LEVELOFANALYSIS_CODE,  
    CSM_CASHFLOW_LEGS_CODE,
    PERIOD_ID,
    REPORTING_BASIS_CODE,
    ' || P_REQUEST_ID || '        AS REQUEST_ID
FROM  ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' 
)WITH NO DATA ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE DISTRIBUTE ON RANDOM';

EXECUTE IMMEDIATE 'INSERT INTO SESSION.' || V_WRK_TBL || '     (
SELECT 
    LINE_NUMBER,
    SUBSIDIARY_CODE ,
    LEDGER_CODE,
    UPPER(REGION_CODE) AS REGION_CODE,
    UPPER(TREATY_LIFE_CHARACTERISTIC_CODE),
    FINANCING_TYPE_CODE,
    IAS39_CODE,
    USGAAP_CODE,
    COINSURANCE_CODE,
    TYPE_OF_BUSINESS_CODE,
    GUARANTEE_CODE,
    POLICY_TYPE_CODE,
    UPPER(NATURE_CODE),
    CLIENT_ID, 
    UPPER(BUSINESSMATURITY_CODE),
    MARKET_UNIT_CODE,
    UPPER(LEVELOFANALYSIS_CODE),  
    UPPER(CSM_CASHFLOW_LEGS_CODE),
    PERIOD_ID,
    UPPER(REPORTING_BASIS_CODE),
    ' || P_REQUEST_ID || '        AS REQUEST_ID
FROM  ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' SRA
)';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    52 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    (
    SELECT 
        SUBSIDIARY_CODE,
        SSD_CF,
        REQUEST_ID,
        LINE_NUMBER
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TSUBSID SUB ON WT.SUBSIDIARY_CODE = CAST(SUB.SSD_CF AS VARCHAR) AND WT.SUBSIDIARY_CODE IS NOT NULL
    WHERE (TRIM(WT.SUBSIDIARY_CODE)<>'''')
    ) SUBCD1
WHERE SUBCD1.SSD_CF IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    53 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM SESSION.' || V_WRK_TBL || ' WT
LEFT OUTER JOIN BI_<env>.TESB  TESB ON (WT.SUBSIDIARY_CODE || ''-'' || WT.LEDGER_CODE) = (TESB.SSD_CF || ''-'' || TESB.ESB_CF) AND (TRIM(WT.SUBSIDIARY_CODE)<>'''') AND (TRIM(WT.LEDGER_CODE)<>'''')
WHERE (TESB.ESB_CF IS NULL OR TESB.SSD_CF IS NULL) ';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    54 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
    FROM 
    ( 
    SELECT 
        REGION_CODE,
        CTYSUP_CF,
        REQUEST_ID,
        LINE_NUMBER
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TCTYSUPL REG ON WT.REGION_CODE = UPPER(REG.CTYSUP_CF) AND REG.LAG_CF = ''E''
    WHERE (TRIM(WT.REGION_CODE)<>'''')
    )REG1
WHERE REG1.CTYSUP_CF IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    55 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    ( 
    SELECT 
        TREATY_LIFE_CHARACTERISTIC_CODE,
        COLVAL_CT,
        REQUEST_ID,
        LINE_NUMBER 
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN  BI_<env>.TBANALL TREATY ON WT.TREATY_LIFE_CHARACTERISTIC_CODE = UPPER(TREATY.COLVAL_CT) AND TREATY.COL_LS = ''LIFTRTTYP_CF'' AND TREATY.LAG_CF = ''E''
    WHERE (TRIM(WT.TREATY_LIFE_CHARACTERISTIC_CODE)<>'''')
    ) TLCHAR
WHERE TLCHAR.COLVAL_CT IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    56 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    (
    SELECT 
        FINANCING_TYPE_CODE,
        COLVAL_CT,
        REQUEST_ID,
        LINE_NUMBER
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN  BI_<env>.TBANALL FINTYP ON WT.FINANCING_TYPE_CODE = CAST(FINTYP.COLVAL_CT AS VARCHAR) AND FINTYP.COL_LS = ''FINTYP_CF'' AND FINTYP.LAG_CF = ''E''
    WHERE (TRIM(WT.FINANCING_TYPE_CODE)<>'''')
    ) FINTYP1
WHERE FINTYP1.COLVAL_CT IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    57 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    (
    SELECT 
        IAS39_CODE,
        COLVAL_CT,
        REQUEST_ID,
        LINE_NUMBER 
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TBANTECL  IAS_CODE ON WT.IAS39_CODE = IAS_CODE.COLVAL_CT AND IAS_CODE.COL_LS = ''ASSFINANCE_CT'' AND IAS_CODE.LAG_CF = ''E''
    WHERE (TRIM(WT.IAS39_CODE)<>'''')
    ) IAS_CODE1
WHERE IAS_CODE1.COLVAL_CT IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    58 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    (
    SELECT 
        USGAAP_CODE,
        COLVAL_CT,
        REQUEST_ID,
        LINE_NUMBER
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TBANTECL  USGAAP ON WT.USGAAP_CODE = USGAAP.COLVAL_CT AND USGAAP.COL_LS = ''USGAAP_CT'' AND USGAAP.LAG_CF = ''E'' 
    WHERE (TRIM(WT.USGAAP_CODE)<>'''')
    ) USGAAP1
WHERE USGAAP1.COLVAL_CT IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    59 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    ( 
    SELECT 
        COINSURANCE_CODE,
        SOB_CF,
        REQUEST_ID,
        LINE_NUMBER 
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TSOB COINS ON WT.COINSURANCE_CODE = COINS.SOB_CF
    WHERE (TRIM(WT.COINSURANCE_CODE)<>'''')
    ) COINS1
WHERE COINS1.SOB_CF IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    60 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM    
    ( 
    SELECT 
        TYPE_OF_BUSINESS_CODE,
        REQUEST_ID,
        LINE_NUMBER
    FROM SESSION.' || V_WRK_TBL || ' WT
    WHERE (TRIM(WT.TYPE_OF_BUSINESS_CODE)<>'''')
    ) TBC
WHERE tBC.TYPE_OF_BUSINESS_CODE NOT IN (''80'',''81'')';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    61 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM SESSION.' || V_WRK_TBL || ' WT
LEFT OUTER JOIN BI_<env>.TGARL  GCODE ON WT.GUARANTEE_CODE = GCODE.GAR_CF AND GCODE.LAG_CF = ''E'' 
WHERE GCODE.GAR_CF IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    62 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM         
    ( 
    SELECT
        POLICY_TYPE_CODE ,
        TOP_CF,
        REQUEST_ID,
        LINE_NUMBER
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TTOPL  POL_TYP ON WT.POLICY_TYPE_CODE = POL_TYP.TOP_CF AND POL_TYP.LAG_CF = ''E''  
    WHERE (TRIM(WT.POLICY_TYPE_CODE)<>'''')
    ) PTYCODE
WHERE PTYCODE.TOP_CF IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    63 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    (
    SELECT 
        NATURE_CODE,
        CTRNATMNE_HD,
        REQUEST_ID,
        LINE_NUMBER
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TCTRNATL  NCODE ON WT.NATURE_CODE = UPPER(NCODE.CTRNATMNE_HD) AND NCODE.LAG_CF = ''E''
    WHERE (TRIM(WT.NATURE_CODE)<>'''')    
    ) NCODE1
WHERE NCODE1.CTRNATMNE_HD IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    64 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    (
    SELECT 
        CLIENT_ID,
        CLI_NF,
        REQUEST_ID,
        LINE_NUMBER 
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TCLIENT  TCLIENT ON WT.CLIENT_ID = TCLIENT.CLI_NF
    WHERE (TRIM(WT.CLIENT_ID)<>'''')    
    ) TCLIENT1
WHERE TCLIENT1.CLI_NF IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    45 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    (
    SELECT 
        BUSINESSMATURITY_CODE,
        CODE,
        REQUEST_ID,
        LINE_NUMBER 
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN  BI_<env>.BUSINESS_MATURITY BMAT ON WT.BUSINESSMATURITY_CODE = UPPER(BMAT.CODE)
    WHERE (TRIM(WT.BUSINESSMATURITY_CODE)<>'''')
    ) BMATC
WHERE BMATC.CODE IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    65 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM 
    (
    SELECT 
        MARKET_UNIT_CODE,
        COLVAL_CT,
        REQUEST_ID,
        LINE_NUMBER 
    FROM SESSION.' || V_WRK_TBL || ' WT
    LEFT OUTER JOIN BI_<env>.TBANTECL  MARUNIT ON WT.MARKET_UNIT_CODE = MARUNIT.COLVAL_CT AND MARUNIT.COL_LS = ''SUBMRK_NT'' AND MARUNIT.LAG_CF = ''E''
    WHERE (TRIM(WT.MARKET_UNIT_CODE)<>'''')
    ) MARUNIT1
WHERE MARUNIT1.COLVAL_CT IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    41 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM SESSION.' || V_WRK_TBL || ' WT
LEFT OUTER JOIN BI_<env>.LEVEL_OF_ANALYSIS  LA ON WT.LEVELOFANALYSIS_CODE = UPPER(LA.CODE) AND DATE(LA.VALID_TO) = DATE(''9999-12-31'')
WHERE LA.CODE IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    43 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM SESSION.' || V_WRK_TBL || ' WT
LEFT OUTER JOIN DWHD1_<env>.V_MAPPING_POSITION MP ON UPPER(WT.CSM_CASHFLOW_LEGS_CODE) = UPPER(MP.MAP_POSITION_CODE)  AND MP.IS_CSM_MAP is true AND DATE(MP.VALID_TO) = DATE(''9999-12-31'')
WHERE MP.MAP_POSITION_CODE IS NULL
';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    51 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM SESSION.' || V_WRK_TBL || ' WT
LEFT OUTER JOIN BI_<env>.PERIOD_TYPE P_TYP ON WT.PERIOD_ID = P_TYP.ID
WHERE P_TYP.ID IS NULL';

EXECUTE IMMEDIATE 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    REQUEST_ID, 
    ERROR_MESSAGE_ID, 
    ERROR_ROW
)
SELECT DISTINCT
    REQUEST_ID,
    46 AS ERROR_MESSAGE_ID,
    LINE_NUMBER
FROM SESSION.' || V_WRK_TBL || ' WT
LEFT OUTER JOIN BI_<env>.REPORTING_BASIS  RBC ON WT.REPORTING_BASIS_CODE = UPPER(RBC.CODE)
WHERE RBC.CODE IS NULL';

EXCEPTION
WHEN OTHERS THEN 
    L_ERR_CD := SUBSTR(SQLERRM,8,5);
    L_ERR_MSG := SQLERRM;
    RAISE EXCEPTION '% Error while executing SQL statement',L_ERR_MSG;
    RETURN L_ERR_CD;
END 
END_PROC;