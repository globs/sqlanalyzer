SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_SAS_TCSM_MICRO_AOC;

CREATE OR REPLACE PROCEDURE SP_LOAD_SAS_TCSM_MICRO_AOC
(
    INTEGER,
    VARCHAR(255),
    DATE,
    VARCHAR(50),
    VARCHAR(50),
    VARCHAR(50)
)
RETURNS INTEGER
LANGUAGE nzplsql 
AS
BEGIN_PROC 

DECLARE 

P_REQUEST_ID ALIAS FOR $1;
P_FILE_NAME ALIAS FOR $2;
P_AS_OF_DATE ALIAS FOR $3;
P_STAGING_SCHEMA ALIAS FOR $4;
P_DWH_SCHEMA ALIAS FOR $5;
P_BI_SCHEMA ALIAS FOR $6;

V_DELETE_WRK  VARCHAR(64000);
V_LOAD_WRK  VARCHAR(64000);
V_DELETE_HISTO VARCHAR(64000);
V_LOAD_HISTO VARCHAR(64000);
V_CURRENT_TS TIMESTAMP;

L_ERR_CD CHAR(5);
L_ERR_MSG VARCHAR(32000);


BEGIN
RAISE NOTICE 'DELETING VALUES FROM WRK_TCSM_MICRO_AOC';

V_DELETE_WRK = 
'
    DELETE FROM ' || P_STAGING_SCHEMA || ' .WRK_TCSM_MICRO_AOC a
';

EXECUTE IMMEDIATE V_DELETE_WRK;
EXECUTE IMMEDIATE 'COMMIT;';

RAISE NOTICE 'INSERTING VALUES INTO WRK_TCSM_MICRO_AOC';

V_LOAD_WRK = 
'
    INSERT INTO ' || P_STAGING_SCHEMA || '.WRK_TCSM_MICRO_AOC 
    (
        SELECT 
            a.SSD_CF,
            a.ESB_CF,
            a.NUMLINE_NT,
            a.BALSHEY_NF,
            a.BALSHRMTH_NF,
            a.BALSHRDAY_NF,
            a.VALPERY_NF,
            a.VALPERMTH_NF,
            a.TRNCOD_CF,
            a.RETAUTGEN_B,
            a.CTR_NF,
            a.END_NT,
            a.SEC_NF,
            a.UWY_NF,
            a.UW_NT,
            a.OCCYEA_NF,
            a.ACY_NF,
            a.SCOSTRMTH_NF,
            a.SCOENDMTH_NF,
            a.CLM_NF,
            a.CUR_CF,
            a.AMT_M,
            a.RETCTR_NF,
            a.RETEND_NT,
            a.RETSEC_NF,
            a.RTY_NF,
            a.RETUW_NT,
            a.PLC_NT,
            a.RETOCCYEA_NF,
            a.RETACY_NF,
            a.RETSCOSTRMTH_NF,
            a.RETSCOENDMTH_NF,
            a.RCL_NF,
            a.RETCUR_CF,
            a.RETAMT_M,
            a.COMMAC_LL,
            a.SPEENTTYP_CF,
            a.SPEENTNAT_CT,
            a.EVT_NT,
            a.REVT_NT,
            a.ACCOUNTING_EVENT_TYPE_ID,
            a.ACCOUNTING_EVENT_TYPE_L4,
            a.ACCOUNTING_EVENT_TYPE_L5,
            a.REPORTING_DT,
            a.ENTITY_ID,
            a.WORKGROUP,
            a.CYCLE_ID,
            a.RUN_ID,
            a.OMEGA_FLG,
            a.BUSINESS_VALIDATION_FLAG,
            a.SCENARIO_ID,
            a.AGGREGATION_FLAG,
            a.TRANSACTION_AMT,
            ROW_NUMBER() OVER (),
            '|| P_REQUEST_ID || ',
            HASH(
                NVL(SSD_CF,NULL, ''0'') || ''-'' || NVL(ESB_CF,NULL, ''0'')|| ''-'' ||
                NVL(BALSHEY_NF,NULL, ''0'') || ''-'' || NVL(BALSHRMTH_NF,NULL, ''0'')|| ''-'' ||
                NVL(BALSHRDAY_NF,NULL, ''0'') || ''-'' || NVL(VALPERY_NF,NULL, ''0'')|| ''-'' ||
                NVL(VALPERMTH_NF,NULL, ''0'') || ''-'' || NVL(TRNCOD_CF,NULL, ''0'')|| ''-'' ||
                NVL(RETAUTGEN_B,NULL, ''0'') || ''-'' || NVL(CTR_NF,NULL, ''0'')|| ''-'' ||
                NVL(END_NT,NULL, ''0'') || ''-'' || NVL(SEC_NF,NULL, ''0'')|| ''-'' ||
                NVL(UWY_NF,NULL, ''0'') || ''-'' || NVL(UW_NT,NULL, ''0'') || ''-'' ||
                NVL(OCCYEA_NF,NULL, ''0'')|| ''-'' || NVL(ACY_NF,NULL, ''0'')|| ''-'' ||
                NVL(SCOSTRMTH_NF,NULL, ''0'') || ''-'' || NVL(SCOENDMTH_NF,NULL, ''0'')|| ''-'' ||
                NVL(CLM_NF,NULL, ''0'') || ''-'' || NVL(CUR_CF,NULL, ''0'')|| ''-'' ||
                NVL(RETCTR_NF,NULL, ''0'') || ''-'' || NVL(RETEND_NT, NULL,''0'')|| ''-'' ||
                NVL(RETSEC_NF,NULL, ''0'') || ''-'' || NVL(RTY_NF,NULL, ''0'')|| ''-'' ||
                NVL(RETUW_NT,NULL, ''0'') || ''-'' || NVL(PLC_NT,NULL, ''0'')|| ''-'' ||
                NVL(RETOCCYEA_NF,NULL, ''0'') || ''-'' || NVL(RETACY_NF,NULL, ''0'')|| ''-'' ||
                NVL(RETSCOSTRMTH_NF,NULL, ''0'') || ''-'' || NVL(RETSCOENDMTH_NF,NULL, ''0'')|| ''-'' ||
                NVL(RCL_NF,NULL, ''0'') || ''-'' || NVL(RETCUR_CF,NULL, ''0'')|| ''-'' ||
                NVL(SPEENTTYP_CF,NULL, ''0'') || ''-'' || NVL(SPEENTNAT_CT,NULL, ''0'')|| ''-'' ||
                NVL(EVT_NT,NULL, ''0'') || ''-'' || NVL(REVT_NT,NULL, ''0'') ' || '
            , 2),
            '|| '''' ||P_FILE_NAME ||'''' ||'
        FROM 
            ' || P_STAGING_SCHEMA || '.STAGING_TCSM_MICRO_AOC  a
    )
';

EXECUTE IMMEDIATE V_LOAD_WRK;
EXECUTE IMMEDIATE 'COMMIT;';

RAISE NOTICE 'DELETING VALUES FROM DWH_TCSM_MICRO_AOC WHERE STAGING_.MOCKUP_RUNID = DWH_.CYCLE_ID';

V_DELETE_HISTO = 
'
    DELETE FROM '|| P_DWH_SCHEMA || '.DWH_TCSM_MICRO_AOC  a
    WHERE a.MOCKUP_RUNID||a.FILE_NAME IN 
    (
        SELECT DISTINCT 
            MOCKUP_RUNID||FILE_NAME
        FROM 
            ' || P_STAGING_SCHEMA || '.WRK_TCSM_MICRO_AOC
    )
';

EXECUTE IMMEDIATE V_DELETE_HISTO;
EXECUTE IMMEDIATE 'COMMIT;';

RAISE NOTICE 'INSERTING VALUES INTO DWH_TCSM_MICRO_AOC';

V_CURRENT_TS := CURRENT_TIMESTAMP;

V_LOAD_HISTO = 
'
    INSERT INTO ' || P_DWH_SCHEMA || '.DWH_TCSM_MICRO_AOC
    (
        SELECT 
            SSD_CF,
            ESB_CF,
            NUMLINE_NT,
            BALSHEY_NF,
            BALSHRMTH_NF,
            BALSHRDAY_NF,
            VALPERY_NF,
            VALPERMTH_NF,
            TRNCOD_CF,
            RETAUTGEN_B,
            CTR_NF,
            END_NT,
            SEC_NF,
            UWY_NF,
            UW_NT,
            OCCYEA_NF,
            ACY_NF,
            SCOSTRMTH_NF,
            SCOENDMTH_NF,
            CLM_NF,
            CUR_CF,
            AMT_M,
            RETCTR_NF,
            RETEND_NT,
            RETSEC_NF,
            RTY_NF,
            RETUW_NT,
            PLC_NT,
            RETOCCYEA_NF,
            RETACY_NF,
            RETSCOSTRMTH_NF,
            RETSCOENDMTH_NF,
            RCL_NF,
            RETCUR_CF,
            RETAMT_M,
            COMMAC_LL,
            SPEENTTYP_CF,
            SPEENTNAT_CT,
            EVT_NT,
            REVT_NT,
            ACCOUNTING_EVENT_TYPE_ID,
            ACCOUNTING_EVENT_TYPE_L4,
            ACCOUNTING_EVENT_TYPE_L5,
            REPORTING_DT,
            ENTITY_ID,
            WORKGROUP,
            CYCLE_ID,
            RUN_ID,
            OMEGA_FLG,
            BUSINESS_VALIDATION_FLAG,
            SCENARIO_ID,
            AGGREGATION_FLAG,
            TRANSACTION_AMT,
            LINE_NUMBER,
            MOCKUP_RUNID,
            COMPOSITE_KEY,
            FILE_NAME,
            '''|| V_CURRENT_TS ||'''
        FROM ' || P_STAGING_SCHEMA || '.WRK_TCSM_MICRO_AOC a
    )
';

EXECUTE IMMEDIATE V_LOAD_HISTO;
EXECUTE IMMEDIATE 'COMMIT;';

EXCEPTION
    WHEN OTHERS THEN L_ERR_CD := substr(SQLERRM,8,5);
    L_ERR_MSG := SQLERRM;
    RAISE EXCEPTION '% Error while executing SQL statement',
    L_ERR_MSG;
RETURN L_ERR_CD;

END;

END_PROC;