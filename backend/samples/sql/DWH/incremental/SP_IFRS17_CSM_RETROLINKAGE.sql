DROP PROCEDURE IFRS17_CSM_RETROLINKAGE;

CREATE OR REPLACE  PROCEDURE IFRS17_CSM_RETROLINKAGE( VARCHAR(7),
VARCHAR(10),
VARCHAR(50), 
VARCHAR(50), 
SMALLINT,
VARCHAR(36),
VARCHAR(7),
SMALLINT,
VARCHAR(9) ) RETURNS INTEGER
LANGUAGE nzplsql AS BEGIN_PROC
DECLARE 

P_CLOSING_QUARTER ALIAS FOR $1; 
P_REAL_CLOSING_DATE ALIAS FOR $2;
P_DWH_SCHEMA ALIAS FOR $3;
P_BI_SCHEMA ALIAS FOR $4;
P_SCENARIO_ID ALIAS FOR $5;
P_ENTITY_ID ALIAS FOR $6;
P_NORM_CODE ALIAS FOR $7;
P_RUN_ID ALIAS FOR $8;
P_INTERVAL ALIAS FOR $9;


V_CLEAR_DATA_QUERY VARCHAR(64000);
V_QUERY VARCHAR(64000);


L_ERR_CD CHAR(5);
L_ERR_MSG VARCHAR(32000);


V_CURRENT_TS TIMESTAMP;

BEGIN
RAISE NOTICE 'Inserting accept data';


V_CLEAR_DATA_QUERY = 'DELETE FROM ' || P_DWH_SCHEMA || '.TCSM_RETROLINKAGE  
						WHERE REPORTING_DT =  VARCHAR_FORMAT(''' || P_REAL_CLOSING_DATE || ''' , ''DD/MM/YYYY'') 
            				AND CLOSING_QUARTER_CODE = ''' || P_CLOSING_QUARTER || '''                      
							AND SCENARIO_ID = ' || P_SCENARIO_ID || ' 
							AND ENTITY_ID = ''' || P_ENTITY_ID || ''' 
							AND RUN_ID= ' || P_RUN_ID || ' 
							AND INTERVAL=''' || P_INTERVAL || ''' ';


EXECUTE IMMEDIATE V_CLEAR_DATA_QUERY;


EXECUTE IMMEDIATE 'COMMIT;';


V_QUERY = 'INSERT INTO   ' || P_DWH_SCHEMA || '.TCSM_RETROLINKAGE	
' || 'SELECT			
' || '	ROW_NUMBER() OVER(ORDER BY INSURANCE_CONTRACT_GROUP_ID ) AS ROW_ID,
' || '	''' || P_CLOSING_QUARTER || ''',
' || '	VARCHAR_FORMAT(''' || P_REAL_CLOSING_DATE || ''',''DD/MM/YYYY'') AS REPORTING_DATE,	
' || '	''' || P_SCENARIO_ID || ''' AS SCENARIO_ID,		
' || '	''' || P_ENTITY_ID || ''' AS ENTITY_ID,		
' || '	INSURANCE_CONTRACT_GROUP_ID,   
' || '	RETCTR_NF || ''_'' || RETSEC_NF || ''_'' || RTY_NF AS REL_INSURANCE_CONTRACT_GROUP_ID,
' || '	''REINS'' AS ASSOCIATION_CD,	
' || '	1 AS WEIGHT_PCT,		
' || '	CESSH_R AS REINS_FIXED_RECOVER_PCT,
' || '	''' || P_NORM_CODE || ''' AS NORM_CODE,		
' || '	''CLOSING'' AS DATA_TYPE,
' || '		' || P_RUN_ID || ' AS RUN_ID,
' || '		''' || P_INTERVAL || ''' AS INTERVAL,
' || '	TO_CHAR(SYSDATE, ''YYYYMMDD'') ASOFDAY
' || 'FROM ' || P_BI_SCHEMA || '.TCESSION
' || 'INNER JOIN 
' || '		(SELECT CTR_NF || ''_'' || SEC_NF  ||''_'' || UWY_NF INSURANCE_CONTRACT_GROUP_ID
' || '			FROM ' || P_BI_SCHEMA || '.TSECTION
' || '			WHERE LOB_CF IN (''30'',''31'') AND SECACCSTS_CT NOT IN (8,9)
' || '			GROUP BY CTR_NF || ''_'' || SEC_NF  ||''_'' || UWY_NF )
' || '	ON CTR_NF || ''_'' || SEC_NF  ||''_'' || UWY_NF = INSURANCE_CONTRACT_GROUP_ID
' || 'WHERE  CESSTS_CF =1';


EXECUTE IMMEDIATE V_QUERY;


EXECUTE IMMEDIATE 'COMMIT;';


EXCEPTION
WHEN OTHERS THEN L_ERR_CD := substr(SQLERRM,8,5);


L_ERR_MSG := SQLERRM;


RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG;


RETURN L_ERR_CD;

END;


END_PROC;