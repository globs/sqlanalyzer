SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_FFX_RATE_CALCULATION;

CREATE OR REPLACE PROCEDURE SP_LOAD_FFX_RATE_CALCULATION(BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64)) RETURNS INTEGER
language nzplsql
AS
BEGIN_PROC
 
DECLARE    
    P_REQUEST_ID                ALIAS FOR $1;
    P_TRG_SCHEMA                ALIAS FOR $2;
    P_TRG_TABLE                 ALIAS FOR $3;       
    L_ERR_CD                    CHAR(5);          
    L_ERR_MSG                   VARCHAR(32000);       
    V_REC                       RECORD;
    V_CLEAN_WRK_ECONOMIC_RATE   VARCHAR(64000);
    V_LOAD_WRK_ECONOMIC_RATE    VARCHAR(64000);
    V_RATES                     VARCHAR(64000);
    
BEGIN
SET ISOLATION TO UR;

V_RATES := 'DROP TABLE SESSION.TMP_OMEGA_RATES IF EXISTS';
EXECUTE IMMEDIATE V_RATES;

V_RATES := 'DECLARE GLOBAL TEMPORARY TABLE SESSION.TMP_OMEGA_RATES AS 
(SELECT DISTINCT
   ADD_MONTHS(TO_DATE(''01/01/''||YEAR(P1.CLOSING_DATE), ''DD/MM/YYYY''), RIGHT(TRIM(QUARTER(P1.CLOSING_DATE)),1) * 3) - 1  AS CLOSING_DATE,
   P1.CURRENCY_FROM,
   P1.CURRENCY_TO,
   P2.FX_RATE 
FROM (SELECT 
	MAX(A.EXC_D) CLOSING_DATE,
   QUARTER(A.EXC_D),
   YEAR(A.EXC_D),
	A.CUR_CF AS CURRENCY_FROM ,
	B.CUR_CF AS CURRENCY_TO
FROM BI_<env>.TBOCURQUOT2 A
LEFT JOIN BI_<env>.TBOCURQUOT2 B ON A.EXC_CT=B.EXC_CT AND A.EXC_D=B.EXC_D AND A.SSD_CF=B.SSD_CF AND A.EXC_CT = ''C'' AND A.SSD_CF=2  
GROUP BY B.CUR_CF,A.CUR_CF,QUARTER(A.EXC_D),YEAR(A.EXC_D)) P1
INNER JOIN (SELECT 
       T1.CUR_CF AS CURRENCY_FROM, 
       T2.CUR_CF AS CURRENCY_TO,
       CASE WHEN T2.EXC_R=0 THEN 0 ELSE (T1.EXC_R/T2.EXC_R) END AS FX_RATE,
       T1.EXC_D AS CLOSING_DATE
   FROM BI_<env>.TBOCURQUOT2 T1 
   LEFT OUTER JOIN BI_<env>.TBOCURQUOT2 T2 ON T1.EXC_CT=T2.EXC_CT AND T1.EXC_D=T2.EXC_D AND T1.SSD_CF=T2.SSD_CF AND T1.EXC_CT = ''C'' AND T1.SSD_CF=2 
   ) P2 ON  P1.CLOSING_DATE=P2.CLOSING_DATE AND P1.CURRENCY_FROM=P2.CURRENCY_FROM AND P1.CURRENCY_TO=P2.CURRENCY_TO
) WITH DATA ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE DISTRIBUTE ON (CURRENCY_FROM,CURRENCY_TO,CLOSING_DATE)
';

RAISE NOTICE 'Preparing Omega FX Rates';
EXECUTE IMMEDIATE V_RATES;

RAISE NOTICE 'Calculating statistics';
EXECUTE IMMEDIATE 'CALL ADMIN_CMD(''RUNSTATS ON TABLE SESSION.TMP_OMEGA_RATES ON ALL COLUMNS AND COLUMNS ((CURRENCY_FROM,CURRENCY_TO,CLOSING_DATE) ) WITH DISTRIBUTION ON ALL COLUMNS SET PROFILE'')';
EXECUTE IMMEDIATE 'COMMIT';
 
V_CLEAN_WRK_ECONOMIC_RATE := 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE REQUEST_ID =' || P_REQUEST_ID || ' ';

RAISE NOTICE 'Executing V_CLEAN_WRK_ECONOMIC_RATE: ''%''',V_CLEAN_WRK_ECONOMIC_RATE; 
EXECUTE IMMEDIATE V_CLEAN_WRK_ECONOMIC_RATE; 

V_LOAD_WRK_ECONOMIC_RATE := 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
    REPORTING_BASIS_ID            ,  
    CLOSING_DATE                  ,
    PARAMETER_TYPE_ID             ,
    SEGMENT_TYPE_ID               ,
    SEGMENT_CODE                  ,
    SUBSIDIARY_CODE               ,
    SUBLEDGER_CODE                ,
    LOB_CODE                      ,
    DOMAIN_CODE                   ,
    ECONOMIC_DATA_AS_OF_DATE      ,
    BASE_CURRENCY_CODE            ,
    CURRENCY_CODE                 ,
    MATURITY_TYPE_ID              ,
    MATURITY                      ,
    USAGE_TYPE_ID                 ,
    RATE                          ,
	DISCOUNT_FACTOR	              ,
    REQUEST_ID
)
SELECT DISTINCT
    E1.REPORTING_BASIS_ID       AS REPORTING_BASIS_ID,
	Q.CLOSING_DATE              AS CLOSING_DATE,
    15                          AS PARAMETER_TYPE_ID,
    E1.SEGMENT_TYPE_ID          AS SEGMENT_TYPE_ID,
    E1.SEGMENT_CODE             AS SEGMENT_CODE,
    E1.SUBSIDIARY_CODE          AS SUBSIDIARY_CODE,
    E1.SUBLEDGER_CODE           AS SUBLEDGER_CODE,
    E1.LOB_CODE                 AS LOB_CODE,
    E1.DOMAIN_CODE              AS DOMAIN_CODE,
    E1.ECONOMIC_DATA_AS_OF_DATE AS ECONOMIC_DATA_AS_OF_DATE,
    Q.CURRENCY_FROM             AS BASE_CURRENCY_CODE,
    Q.CURRENCY_TO               AS CURRENCY_CODE ,
    E1.MATURITY_TYPE_ID         AS MATURITY_TYPE_ID,
	E1.MATURITY                 AS MATURITY,    
    E1.USAGE_TYPE_ID            AS USAGE_TYPE_ID,
    Q.FX_RATE*(POWER((1 + E2.RATE),(CAST(E1.MATURITY AS DECFLOAT)/ 12))/(POWER((1 + E1.RATE),(CAST(E1.MATURITY AS DECFLOAT)/ 12)))) AS RATE,
    CASE WHEN POWER((1 + Q.FX_RATE*(POWER((1 + E2.RATE),(CAST(E1.MATURITY AS DECFLOAT)/ 12))/(POWER((1 + E1.RATE),(CAST(E1.MATURITY AS DECFLOAT)/ 12))))),(DECFLOAT(E1.MATURITY) / 12))  = 0 THEN 0 
		ELSE  (1 /( POWER((1 + Q.FX_RATE*(POWER((1 + E2.RATE),(CAST(E1.MATURITY AS DECFLOAT)/ 12))/(POWER((1 + E1.RATE),(CAST(E1.MATURITY AS DECFLOAT)/ 12))))),(DECFLOAT(E1.MATURITY) / 12)) )) 
	END AS DISCOUNT_FACTOR,	
    ' || P_REQUEST_ID || '      AS REQUEST_ID
FROM SESSION.TMP_OMEGA_RATES Q 
INNER JOIN BI_<env>.ECONOMIC_RATE E1 ON (E1.CURRENCY_CODE = Q.CURRENCY_FROM AND E1.PARAMETER_TYPE_ID=14 AND DATE(E1.VALID_TO) = ''9999-12-31'' AND E1.CLOSING_DATE= DATE (Q.CLOSING_DATE)) 
INNER JOIN BI_<env>.ECONOMIC_RATE E2 ON (E2.CURRENCY_CODE = Q.CURRENCY_TO AND E1.MATURITY = E2.MATURITY AND E1.REPORTING_BASIS_ID = E2.REPORTING_BASIS_ID 
    AND IFNULL(E1.SEGMENT_CODE,''0'') = IFNULL(E2.SEGMENT_CODE ,''0'')
    AND IFNULL(E1.REPORTING_BASIS_ID,0) = IFNULL(E2.REPORTING_BASIS_ID,0) 
    AND IFNULL(E1.USAGE_TYPE_ID ,0) = IFNULL(E2.USAGE_TYPE_ID ,0)
    AND IFNULL(E1.SEGMENT_TYPE_ID,0) = IFNULL(E2.SEGMENT_TYPE_ID ,0)
    AND IFNULL(E1.SUBSIDIARY_CODE,0) = IFNULL(E2.SUBSIDIARY_CODE ,0)
    AND IFNULL(E1.SUBLEDGER_CODE,0) = IFNULL(E2.SUBLEDGER_CODE ,0)
    AND IFNULL(E1.LOB_CODE,''0'') = IFNULL(E2.LOB_CODE  ,''0'')
    AND IFNULL(E1.DOMAIN_CODE,''0'') = IFNULL(E2.DOMAIN_CODE,''0'')  
    AND IFNULL(E1.ECONOMIC_DATA_AS_OF_DATE,''9999-12-31'') = IFNULL(E2.ECONOMIC_DATA_AS_OF_DATE,''9999-12-31'')
    AND E1.CLOSING_DATE = E2.CLOSING_DATE AND E2.PARAMETER_TYPE_ID=E1.PARAMETER_TYPE_ID AND DATE(E1.VALID_TO) = DATE(E2.VALID_TO))
';

RAISE NOTICE 'Executing V_LOAD_WRK_ECONOMIC_RATE: ''%''',V_LOAD_WRK_ECONOMIC_RATE; 
EXECUTE IMMEDIATE V_LOAD_WRK_ECONOMIC_RATE; 

V_LOAD_WRK_ECONOMIC_RATE := 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
    REPORTING_BASIS_ID            ,  
    CLOSING_DATE                  ,
    PARAMETER_TYPE_ID             ,
    SEGMENT_TYPE_ID               ,
    SEGMENT_CODE                  ,
    SUBSIDIARY_CODE               ,
    SUBLEDGER_CODE                ,
    LOB_CODE                      ,
    DOMAIN_CODE                   ,
    ECONOMIC_DATA_AS_OF_DATE      ,
    BASE_CURRENCY_CODE            ,
    CURRENCY_CODE                 ,
    MATURITY_TYPE_ID              ,
    MATURITY                      ,
    USAGE_TYPE_ID                 ,
    RATE                          ,
	DISCOUNT_FACTOR	              ,
    REQUEST_ID
)
SELECT DISTINCT
    E1.REPORTING_BASIS_ID       AS REPORTING_BASIS_ID,
	Q.CLOSING_DATE              AS CLOSING_DATE,
    15                          AS PARAMETER_TYPE_ID,
    E1.SEGMENT_TYPE_ID          AS SEGMENT_TYPE_ID,
    E1.SEGMENT_CODE             AS SEGMENT_CODE,
    E1.SUBSIDIARY_CODE          AS SUBSIDIARY_CODE,
    E1.SUBLEDGER_CODE           AS SUBLEDGER_CODE,
    E1.LOB_CODE                 AS LOB_CODE,
    E1.DOMAIN_CODE              AS DOMAIN_CODE,
    E1.ECONOMIC_DATA_AS_OF_DATE AS ECONOMIC_DATA_AS_OF_DATE,
    Q.CURRENCY_FROM             AS BASE_CURRENCY_CODE,
    Q.CURRENCY_TO               AS CURRENCY_CODE ,
    E1.MATURITY_TYPE_ID         AS MATURITY_TYPE_ID,
	0                           AS MATURITY,  
    E1.USAGE_TYPE_ID            AS USAGE_TYPE_ID,  
    Q.FX_RATE                   AS RATE,
    CASE WHEN POWER((1 + Q.FX_RATE),(DECFLOAT(0) / 12))  = 0 THEN 0 ELSE (1 /( POWER((1 + Q.FX_RATE),(DECFLOAT(0) / 12)) )) END AS DISCOUNT_FACTOR,	
    ' || P_REQUEST_ID || '      AS REQUEST_ID
FROM SESSION.TMP_OMEGA_RATES Q 
INNER JOIN BI_<env>.ECONOMIC_RATE E1 ON (E1.CURRENCY_CODE = Q.CURRENCY_FROM AND E1.PARAMETER_TYPE_ID=14 AND DATE(E1.VALID_TO) = ''9999-12-31'' AND E1.CLOSING_DATE= DATE (Q.CLOSING_DATE)) 
INNER JOIN BI_<env>.ECONOMIC_RATE E2 ON (E2.CURRENCY_CODE = Q.CURRENCY_TO AND E1.MATURITY = E2.MATURITY AND E1.REPORTING_BASIS_ID = E2.REPORTING_BASIS_ID 
    AND IFNULL(E1.SEGMENT_CODE,''0'') = IFNULL(E2.SEGMENT_CODE ,''0'')
    AND IFNULL(E1.REPORTING_BASIS_ID,0) = IFNULL(E2.REPORTING_BASIS_ID,0) 
    AND IFNULL(E1.USAGE_TYPE_ID ,0) = IFNULL(E2.USAGE_TYPE_ID ,0)
    AND IFNULL(E1.SEGMENT_TYPE_ID,0) = IFNULL(E2.SEGMENT_TYPE_ID ,0)
    AND IFNULL(E1.SUBSIDIARY_CODE,0) = IFNULL(E2.SUBSIDIARY_CODE ,0)
    AND IFNULL(E1.SUBLEDGER_CODE,0) = IFNULL(E2.SUBLEDGER_CODE ,0)
    AND IFNULL(E1.LOB_CODE,''0'') = IFNULL(E2.LOB_CODE  ,''0'')
    AND IFNULL(E1.DOMAIN_CODE,''0'') = IFNULL(E2.DOMAIN_CODE,''0'')  
    AND IFNULL(E1.ECONOMIC_DATA_AS_OF_DATE,''9999-12-31'') = IFNULL(E2.ECONOMIC_DATA_AS_OF_DATE,''9999-12-31'')
    AND E1.CLOSING_DATE = E2.CLOSING_DATE AND E2.PARAMETER_TYPE_ID=E1.PARAMETER_TYPE_ID AND DATE(E1.VALID_TO) = DATE(E2.VALID_TO))
';

RAISE NOTICE 'Executing V_LOAD_WRK_ECONOMIC_RATE: ''%''',V_LOAD_WRK_ECONOMIC_RATE; 
EXECUTE IMMEDIATE V_LOAD_WRK_ECONOMIC_RATE; 

EXECUTE IMMEDIATE 'DROP TABLE SESSION.TMP_OMEGA_RATES IF EXISTS';
EXECUTE IMMEDIATE 'COMMIT';

EXCEPTION WHEN OTHERS THEN
	L_ERR_CD := SUBSTR(SQLERRM,8,5); 
	L_ERR_MSG := SQLERRM;
	RAISE EXCEPTION '% Error while executing SQL statement',L_ERR_MSG;
	RETURN L_ERR_CD;

END;

END_PROC
;
