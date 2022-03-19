DROP PROCEDURE IFRS17_CSM_QUOTES;

CREATE OR REPLACE PROCEDURE IFRS17_CSM_QUOTES(VARCHAR(7),
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
V_FILTER_SY VARCHAR(64000);


L_ERR_CD CHAR(5);
L_ERR_MSG VARCHAR(32000);

V_CURRENT_TS TIMESTAMP;

BEGIN
RAISE NOTICE 'Inserting accept data';


V_CLEAR_DATA_QUERY = 'DELETE FROM ' || P_DWH_SCHEMA || '.TCSM_QUOTES 
								WHERE REPORTING_DT = VARCHAR_FORMAT(''' || P_REAL_CLOSING_DATE || ''',''DD/MM/YYYY'')
									AND CLOSING_QUARTER_CODE = ''' || P_CLOSING_QUARTER || ''' 
									AND SCENARIO_ID = ' || P_SCENARIO_ID || ' 
									AND ENTITY_ID = ''' || P_ENTITY_ID || '''
									AND RUN_ID   = ' || P_RUN_ID || '
									AND INTERVAL  = ''' || P_INTERVAL || '''  ';


EXECUTE IMMEDIATE V_CLEAR_DATA_QUERY;


EXECUTE IMMEDIATE 'COMMIT;';

IF P_INTERVAL = 'SEMI-YEAR' OR P_INTERVAL='HYTD'
	THEN V_FILTER_SY = 'BETWEEN DATE(''' || P_REAL_CLOSING_DATE || ''') 
' || '							- CASE 	WHEN QUARTER(DATE(''' || P_REAL_CLOSING_DATE || ''') ) IN (1,3) 
' || '										 THEN 3
' || '									WHEN QUARTER(DATE(''' || P_REAL_CLOSING_DATE || ''') ) IN (2,4) 
' || '										 THEN 6 
' || '									END MONTHS  
' || '					AND DATE(''' || P_REAL_CLOSING_DATE || ''')	';
ELSEIF P_INTERVAL = 'ANNUAL' OR P_INTERVAL='YTD' 
	THEN V_FILTER_SY = 'BETWEEN DATE(''' || P_REAL_CLOSING_DATE || ''') 
' || '							- CASE QUARTER(DATE(''' || P_REAL_CLOSING_DATE || ''') )
' || '										WHEN 1 THEN 3
' || '										WHEN 2 THEN 6 
' || '										WHEN 3 THEN 9 
' || '										WHEN 4 THEN 12 
' || '									END MONTHS  
' || '					AND DATE(''' || P_REAL_CLOSING_DATE || ''')	';
END IF;


V_QUERY = 'INSERT INTO  ' || P_DWH_SCHEMA || '.TCSM_QUOTES 
' || 'WITH TRT AS (
' || '		SELECT *
' || '			FROM ' || P_BI_SCHEMA || '.TCSM_TRT_CHARACS
' || '			WHERE REPORTING_DT = VARCHAR_FORMAT(''' || P_REAL_CLOSING_DATE || ''' , ''DD/MM/YYYY'') 
' || '				AND CLOSING_QUARTER_CODE = ''' || P_CLOSING_QUARTER || '''
' || '				AND SCENARIO_ID   = ' || P_SCENARIO_ID || ' 
' || '				AND ENTITY_ID =''' || P_ENTITY_ID || '''
' || '				AND INTERVAL =''' || P_INTERVAL || '''
' || '				AND RUN_ID   = ' || P_RUN_ID || '  )
' || 'SELECT ROW_NUMBER() OVER (ORDER BY QUOTE_DT) as ROW_ID,
' || '	''' || P_CLOSING_QUARTER || ''',
' || '	VARCHAR_FORMAT( ''' || P_REAL_CLOSING_DATE || ''' ,''DD/MM/YYYY'') AS REPORTING_DT,   
' || '	CAST(SCENARIO_ID AS SMALLINT) AS SCENARIO_ID, 
' || '	CAST(QUOTE_DT AS VARCHAR(10)) AS QUOTE_DT, 
' || '	CAST(QUOTE_ID AS VARCHAR(36)) AS QUOTE_ID, 
' || '	MATURITY_DT  AS MATURITY_DT, 
' || '	QUOTE_RT, 
' || '	CAST(RISK_FACTOR_CATEGORY_CD AS VARCHAR(36)) AS RISK_FACTOR_CATEGORY_CD, 
' || '	CAST(ENTITY_CD AS VARCHAR(36)) AS ENTITY_CD, 
' || '	CURRENCY_CD, 
' || '	CAST(RATEINDEX_CT AS VARCHAR(20)) AS RATEINDEX_CT ,
' || '	EC_FLAG ,
' || '	''' || P_NORM_CODE || ''',
' || '	' || P_RUN_ID || ' AS RUN_ID,
' || '	''' || P_INTERVAL || ''' AS INTERVAL,
' || '	TO_CHAR(SYSDATE, ''YYYYMMDD'') AS ASOFDAY 
' || 'FROM ( 
' || 'SELECT DISTINCT  ' || P_SCENARIO_ID || ' AS SCENARIO_ID,
' || '	VARCHAR_FORMAT(NEXT_QUARTER(C.EXC_D)-1,''DD/MM/YYYY'')  AS QUOTE_DT, 
' || '	CASE WHEN C.EXC_CT=''C'' THEN ''SPOT'' 
' || '			 WHEN C.EXC_CT=''M'' THEN ''AVG_SPOT''							
' || '		END	|| ''_'' || B.SSDCUR_CF  || ''_'' || C.CUR_CF AS QUOTE_ID, 
' || '	NULL AS MATURITY_DT, 
' || '	QUOT2.EXC_R AS QUOTE_RT, 
' || '	''FX'' AS RISK_FACTOR_CATEGORY_CD, 
' || '	''' || P_ENTITY_ID || ''' AS ENTITY_CD, 
' || '	NULL AS CURRENCY_CD, 
' || '	NULL AS RATEINDEX_CT,
' || '	NULL EC_FLAG
' || 'FROM ' || P_BI_SCHEMA || '.TBOCURQUOT2 QUOT2
' || 'INNER JOIN (  SELECT    YEAR(EXC_D)*10+QUARTER(EXC_D),
' || '				CUR_CF, 
' || '				SSD_CF,
' || '				EXC_CT, 
' || '				MAX(A.EXC_D) EXC_D
' || '				FROM  ' || P_BI_SCHEMA || '.TBOCURQUOT2 A 
' || '				WHERE  EXC_CT IN (''C'',''M'') 
' || '				AND EXC_D<=''' || P_REAL_CLOSING_DATE || '''
' || '				GROUP BY 
' || '				 YEAR(EXC_D)*10+QUARTER(EXC_D),
' || '						CUR_CF, 
' || '						SSD_CF,
' || '						EXC_CT
' || '			) C
' || '	ON  C.CUR_CF = QUOT2.CUR_CF
' || '	AND	C.EXC_D = QUOT2.EXC_D
' || '	AND	C.SSD_CF = QUOT2.SSD_CF
' || '	AND	C.EXC_CT = QUOT2.EXC_CT
' || 'INNER JOIN ' || P_BI_SCHEMA || '.TSUBSID B ON 
' || '			QUOT2.SSD_CF = B.SSD_CF	
' || 'WHERE  NEXT_QUARTER(C.EXC_D)-1  ' || V_FILTER_SY || '
' || 'UNION ALL 
' || 'SELECT 
' || '    ' || P_SCENARIO_ID || ' AS SCENARIO_ID,
' || '    VARCHAR_FORMAT(CLOSING_DATE,''DD/MM/YYYY'') AS QUOTE_DT,
' || '    QUOTE_ID,
' || '	VARCHAR_FORMAT(LAST_DAY(CLOSING_DATE + Q.MATURITY MONTH), ''DD/MM/YYYY'') AS MATURITY_DT,
' || '	QUOTE_RT,
' || '	''IR'' AS RISK_FACTOR_CATEGORY_CD,
' || '	''' || P_ENTITY_ID || ''' AS ENTITY_CD,
' || '	CURRENCY_CD,
' || '	RATEINDEX_CT,
' || '	EC_FLAG
' || 'FROM(
' || '	SELECT  
' || '	    ECR.CLOSING_DATE,
' || '	    ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE ||RIGHT(''000'' ||ECR.MATURITY|| ''M'',5) AS QUOTE_ID,
' || '		MATURITY,
' || '		ECR.RATE AS QUOTE_RT,
' || '		ECR.CURRENCY_CODE AS CURRENCY_CD, 
' || '		ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE || ''_Q'' ||QUARTER(ECR.CLOSING_DATE)||YEAR(ECR.CLOSING_DATE) AS RATEINDEX_CT,
' || '		''CC'' EC_FLAG
' || '	FROM ' || P_BI_SCHEMA || '.ECONOMIC_RATE ECR 
' || '	INNER JOIN ' || P_BI_SCHEMA || '.PARAMETER_TYPE PT 
' || '		ON PT.ID = ECR.PARAMETER_TYPE_ID
' || '	INNER JOIN ' || P_BI_SCHEMA || '.SEGMENT_TYPE ST 
' || '		ON ST.ID = ECR.SEGMENT_TYPE_ID
' || '	INNER JOIN ' || P_BI_SCHEMA || '.MATURITY_TYPE MT 
' || '		ON MT.ID = ECR.MATURITY_TYPE_ID
' || '	INNER JOIN  TRT
' || '	ON TRT.FK_CC= ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE 
' || '				|| ''_Q'' ||QUARTER(ECR.CLOSING_DATE)||YEAR(ECR.CLOSING_DATE) 
' || '	WHERE ECR.LOB_CODE = 30
' || '	AND ECR.MATURITY_TYPE_ID=2
' || '	AND PT.CODE = ''ZCR''	
' || '	AND ECR.VALID_TO = ''9999-12-31'' 
' || '	AND ECR.SUPP_DATE = ''9999-12-31'' 
' || '	AND ECR.SEGMENT_TYPE_ID = 4  
' || '	AND ECR.SOURCE_IDENTIFIER IS NOT NULL 
' || '	AND  ECR.CLOSING_DATE  ' || V_FILTER_SY || '	
' || '	UNION ALL  
' || '	SELECT  
' || '	    ECR.CLOSING_DATE,
' || '	    ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE || ''_FWD_'' ||RIGHT(''000'' ||ECR.MATURITY|| ''M'',5) AS QUOTE_ID,
' || '		MATURITY,
' || '		ECR.RATE AS QUOTE_RT,
' || '		ECR.CURRENCY_CODE AS CURRENCY_CD,
' || '		ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE 
' || '			|| ''_Q'' ||QUARTER(ECR.ORIGIN_CLOSING_DATE)||YEAR(ECR.ORIGIN_CLOSING_DATE) 
' || '			|| ''_Q'' ||QUARTER(ECR.CLOSING_DATE)||YEAR(ECR.CLOSING_DATE) AS RATEINDEX_CT,
' || '		''FWD''
' || '	FROM ' || P_BI_SCHEMA || '.ECONOMIC_RATE ECR
' || '	INNER JOIN ' || P_BI_SCHEMA || '.PARAMETER_TYPE PT ON PT.ID = ECR.PARAMETER_TYPE_ID  
' || '	INNER JOIN ' || P_BI_SCHEMA || '.SEGMENT_TYPE ST ON ST.ID = ECR.SEGMENT_TYPE_ID
' || '	INNER JOIN  TRT
' || '	ON TRT.FK_CF= ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE 
' || '			|| ''_Q'' ||QUARTER(ECR.ORIGIN_CLOSING_DATE)||YEAR(ECR.ORIGIN_CLOSING_DATE) 
' || '			|| ''_Q'' ||QUARTER(ECR.CLOSING_DATE)||YEAR(ECR.CLOSING_DATE)
' || '	WHERE LOB_CODE = 30 
' || '	AND PT.CODE =''FWD'' 
' || '	AND ECR.SEGMENT_TYPE_ID = 4  
' || '	AND ECR.MATURITY_TYPE_ID=2
' || '	AND SOURCE_IDENTIFIER IS NOT NULL
' || '	AND ECR.VALID_TO = ''9999-12-31'' AND ECR.SUPP_DATE = ''9999-12-31''
' || '	AND CLOSING_DATE =''' || P_REAL_CLOSING_DATE || '''
' || '	AND ORIGIN_CLOSING_DATE  ' || V_FILTER_SY || '
' || '	UNION ALL  
' || '	SELECT    
' || '	    ECR.CLOSING_DATE,
' || '	    ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE || ''_FWD_'' ||RIGHT(''000'' ||ECR.MATURITY|| ''M'',5) AS QUOTE_ID,
' || '		MATURITY,
' || '		ECR.RATE AS QUOTE_RT,
' || '		ECR.CURRENCY_CODE AS CURRENCY_CD,
' || '		ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE 
' || '			|| ''_Q'' ||QUARTER(ECR.ORIGIN_CLOSING_DATE)||YEAR(ECR.ORIGIN_CLOSING_DATE)  AS RATEINDEX_CT,
' || '		''LKI'' 
' || '	FROM ' || P_BI_SCHEMA || '.ECONOMIC_RATE ECR
' || '	INNER JOIN ' || P_BI_SCHEMA || '.PARAMETER_TYPE PT ON PT.ID = ECR.PARAMETER_TYPE_ID  
' || '	INNER JOIN ' || P_BI_SCHEMA || '.SEGMENT_TYPE ST ON ST.ID = ECR.SEGMENT_TYPE_ID
' || '	INNER JOIN  TRT
' || '	ON TRT.RATEINDEX_CT= ECR.CURRENCY_CODE || ''_LI'' ||SEGMENT_CODE 
' || '				|| ''_Q'' ||QUARTER(ECR.ORIGIN_CLOSING_DATE)||YEAR(ECR.ORIGIN_CLOSING_DATE) 
' || '	WHERE LOB_CODE = 30 
' || '	AND ECR.PARAMETER_TYPE_ID = 14
' || '	AND ECR.SEGMENT_TYPE_ID = 4  
' || '	AND ECR.MATURITY_TYPE_ID=2
' || '	AND ECR.VALID_TO = ''9999-12-31'' AND ECR.SUPP_DATE = ''9999-12-31''
' || '	AND CLOSING_DATE = ''' || P_REAL_CLOSING_DATE || ''' ) Q
' || 'INNER JOIN
' || '	(SELECT  QUARTER(CLOSING_DATE) Q_ID,MATURITY
' || '		FROM ' || P_BI_SCHEMA || '.ECONOMIC_RATE ECR
' || '		WHERE  ECR.MATURITY <= 69- 3 *(CASE quarter(ECR.CLOSING_DATE) WHEN 4   THEN 0
' || '			                        		ELSE (quarter(ECR.CLOSING_DATE)-1) 
' || '			                    		END)
' || '		AND MATURITY % 3=0
' || '		GROUP BY QUARTER(CLOSING_DATE),MATURITY
' || '		UNION ALL 
' || '		SELECT DISTINCT QUARTER(CLOSING_DATE),MATURITY
' || '		FROM ' || P_BI_SCHEMA || '.ECONOMIC_RATE ECR
' || '		WHERE ECR.MATURITY > 80 - 3*(quarter(ECR.closing_date)-1)
' || '		AND (MONTH(CLOSING_DATE)+MATURITY) % 12=0
' || '		GROUP BY QUARTER(CLOSING_DATE),MATURITY) PERIODICITY
' || '	ON QUARTER(CLOSING_DATE) = Q_ID
' || '	AND Q.MATURITY = PERIODICITY.MATURITY
' || '	)A';


EXECUTE IMMEDIATE V_QUERY;


EXECUTE IMMEDIATE 'COMMIT;';


EXCEPTION
WHEN OTHERS THEN L_ERR_CD := substr(SQLERRM,8,5);


L_ERR_MSG := SQLERRM;


RAISE EXCEPTION '% Error while executing SQL statement',
L_ERR_MSG;


RETURN L_ERR_CD;

END;


END_PROC;