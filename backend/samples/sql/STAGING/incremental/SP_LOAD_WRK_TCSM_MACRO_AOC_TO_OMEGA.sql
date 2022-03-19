SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_WRK_TCSM_MACRO_AOC_TO_OMEGA;

CREATE OR REPLACE PROCEDURE SP_LOAD_WRK_TCSM_MACRO_AOC_TO_OMEGA
(
    VARCHAR(7),
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

P_CLOSING_QUARTER ALIAS FOR $1;
P_AS_OF_DATE ALIAS FOR $2;
P_STAGING_SCHEMA ALIAS FOR $3;
P_DWH_SCHEMA ALIAS FOR $4;
P_BI_SCHEMA ALIAS FOR $5;

V_DELETE_WRK VARCHAR(64000);
V_LOAD_WRK VARCHAR(64000);

L_ERR_CD CHAR(5);
L_ERR_MSG VARCHAR(32000);


BEGIN
RAISE NOTICE 'DELETING VALUES FROM WRK_TCSM_MICRO_AOC';

V_DELETE_WRK = 
'
    DELETE FROM '|| P_STAGING_SCHEMA || '.WRK_TCSM_MACRO_AOC_TO_OMEGA a
';

EXECUTE IMMEDIATE V_DELETE_WRK;
EXECUTE IMMEDIATE 'COMMIT;';

V_LOAD_WRK = 
'
    INSERT INTO '|| P_STAGING_SCHEMA || '.WRK_TCSM_MACRO_AOC_TO_OMEGA
    (
        SELECT
            a.SSD_CF,
            a.ESB_CF,
            a.rownum,
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
            a.MOCKUP_RUNID || '' - '' || a.rownum,
            a.SPEENTTYP_CF,
            a.SPEENTNAT_CT,
            a.EVT_NT,
            a.REVT_NT,
            a.MOCKUP_RUNID,
            a.COMPOSITE_KEY,
            a.FILE_NAME
        FROM
        (
            SELECT
                a.SSD_CF,
                a.ESB_CF,
                ROW_NUMBER() OVER() AS rownum,
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
                SUM(a.AMT_M) AS AMT_M,
                CASE
                    WHEN a.RETCTR_NF = '''' THEN NULL
                    ELSE a.RETCTR_NF
                END RETCTR_NF,
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
                SUM(a.RETAMT_M) AS RETAMT_M,
                a.COMMAC_LL,
                a.SPEENTTYP_CF,
                a.SPEENTNAT_CT,
                a.EVT_NT,
                a.REVT_NT,
                a.MOCKUP_RUNID,
                a.COMPOSITE_KEY,
                a.FILE_NAME
            FROM
                '|| P_STAGING_SCHEMA || '.WRK_TCSM_MICRO_AOC a
            WHERE
                a.OMEGA_FLG = 1
            GROUP BY
                a.SSD_CF,
                a.ESB_CF,
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
                a.SPEENTTYP_CF,
                a.SPEENTNAT_CT,
                a.EVT_NT,
                a.REVT_NT,
                a.MOCKUP_RUNID,
                a.COMPOSITE_KEY,
                a.FILE_NAME
        ) a
    )
';

EXECUTE IMMEDIATE V_LOAD_WRK;
EXECUTE IMMEDIATE 'COMMIT;';

EXCEPTION
    WHEN OTHERS THEN L_ERR_CD := substr(SQLERRM,8,5);
        L_ERR_MSG := SQLERRM;
        RAISE EXCEPTION '% Error while executing SQL statement',
        L_ERR_MSG;
RETURN L_ERR_CD;

END;

END_PROC;