SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_REJECT_DWH_PROJECTION;

CREATE OR REPLACE PROCEDURE SP_LOAD_REJECT_DWH_PROJECTION (BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64)) RETURNS INTEGER 
LANGUAGE nzplsql AS 
BEGIN_PROC 

DECLARE 
    P_REQUEST_ID	        ALIAS FOR $1;
    P_SRC_SCHEMA			ALIAS FOR $2;
    P_SRC_TABLE				ALIAS FOR $3;
    P_TRG_SCHEMA			ALIAS FOR $4;
    P_TRG_TABLE				ALIAS FOR $5;
    P_SOURCE                ALIAS FOR $6;
    L_ERR_CD				CHAR(5);
    L_ERR_MSG				VARCHAR(32000);
    V_REC					RECORD;
    V_INSERT_QUERY_US       VARCHAR(32000);
    V_INSERT_QUERY_APAC     VARCHAR(32000);
    V_INSERT_QUERY_CAN      VARCHAR(32000);
    V_INSERT_QUERY_FRGE     VARCHAR(32000);
    V_INSERT_QUERY_UK       VARCHAR(32000);
    V_INSERT_QUERY_MU       VARCHAR(32000);
    V_INSERT_QUERY_LRM      VARCHAR(32000);

BEGIN
SET ISOLATION TO UR;

V_INSERT_QUERY_US :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    OMEGA_TREATY_NUMBER                     ,
    OMEGA_SECTION                           ,
    AOC_STEP                                ,
    SENSITIVITY_TYPE                        ,
    SENSITIVITY_VALUE                       ,
    PRODUCT									,
    BASIS                                   ,
    CREATED_BY                              ,
    CREATED_DATE                            ,
    REQUEST_ID                              ,
	ERROR_MESSAGE_ID                        ,
    CLOSING_DATE                            ,
    BUSINESS_MATURITY                       ,
    RETROOMEGATREATYNUMBER                  ,
    RETROOMEGASECTIONNUMBER                 ,
    LE                                      ,
    A_R                                     ,
    LE_HOP                                  ,
	SPLIT
)
SELECT
    STG.OMEGATREATYNUMBER       AS OMEGA_TREATY_NUMBER,
    STG.OMEGASECTIONNUMBER      AS OMEGA_SECTION, 
    STG.AOC_STEP_STEPS          AS AOC_STEP,
    STG.SENSITIVITY_TYPE        AS SENSITIVITY_TYPE,
    STG.SENSITIVITY_VOLUME      AS SENSITIVITY_VALUE,
	STG.PRODUCT                 AS PRODUCT,
    ''IFRS17 Global''           AS BASIS,
    '''||P_SOURCE||'''          AS CREATED_BY,
    CAST(sysdate AS TIMESTAMP)  AS CREATED_DATE,
    ERR.REQUEST_ID              AS REQUEST_ID,
	ERR.ERROR_MESSAGE_ID        AS ERROR_MESSAGE_ID,  
    STG.VALUATION_DATE          AS CLOSING_DATE,
    DM.NAME                     AS BUSINESS_MATURITY,           
    CASE WHEN STG.RETROOMEGATREATYNUMBER <> '''' THEN STG.RETROOMEGATREATYNUMBER END    AS RETROOMEGATREATYNUMBER ,
    CASE WHEN STG.RETROOMEGASECTIONNUMBER <> '''' THEN STG.RETROOMEGASECTIONNUMBER END  AS RETROOMEGASECTIONNUMBER ,         
    STG.LE                      AS LE,
    STG.A_R                     AS A_R,
    STG.LE_HOP                  AS LE_HOP,
	STG.MODELLED_PERM_TERM_IND  AS SPLIT              
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
INNER JOIN DELIVERY_<env>.UPLOAD_ERROR_LOG ERR ON (ERR.ERROR_ROW = STG.LINE_NUMBER AND ERR.REQUEST_ID = ' || P_REQUEST_ID || ')
LEFT OUTER JOIN BI_<env>.BUSINESS_MATURITY DM ON (DM.CODE = (CASE
                                                                WHEN (UPPER(STG.NEWBUSINESSIND) = ''EB'') THEN ''F''
                                                                WHEN (UPPER(STG.NEWBUSINESSIND) = ''FNB'') THEN ''FNB''
                                                                WHEN (UPPER(STG.NEWBUSINESSIND) = ''NB'') THEN ''B''
                                                                WHEN (UPPER(STG.NEWBUSINESSIND) = ''ANB'') THEN ''B''
                                                            ELSE STG.NEWBUSINESSIND             END))
GROUP BY 
STG.OMEGATREATYNUMBER      
,STG.OMEGASECTIONNUMBER     
,STG.AOC_STEP_STEPS         
,STG.SENSITIVITY_TYPE       
,STG.SENSITIVITY_VOLUME   
,STG.PRODUCT  
,''IFRS17 Global''           
,'''||P_SOURCE||'''               
,CAST(sysdate AS TIMESTAMP) 
,ERR.REQUEST_ID             
,ERR.ERROR_MESSAGE_ID       
,STG.VALUATION_DATE         
,DM.NAME                    
,CASE WHEN STG.RETROOMEGATREATYNUMBER <> '''' THEN STG.RETROOMEGATREATYNUMBER END  
,CASE WHEN STG.RETROOMEGASECTIONNUMBER <> '''' THEN STG.RETROOMEGASECTIONNUMBER END
,STG.LE        
,STG.A_R       
,STG.LE_HOP    
,STG.MODELLED_PERM_TERM_IND
';    

V_INSERT_QUERY_APAC :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    OMEGA_TREATY_NUMBER                 ,
    OMEGA_SECTION                       ,
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    GROSS_ASSUMED_OMEGA_SECTION         ,
    SPLIT                               ,
    AOC_STEP                            ,
    SENSITIVITY_TYPE                    ,
    SENSITIVITY_VALUE                   ,
    POLICY_UWY                          ,
    POSITION                            ,
    CURRENCY                            ,
    PRODUCT                             ,
    BASIS                               ,
    CREATED_BY                          ,
    CREATED_DATE                        ,
    REQUEST_ID                          ,
    ERROR_MESSAGE_ID                    ,
    CLOSING_DATE                        ,
    BUSINESS_MATURITY                   ,
    RETROOMEGATREATYNUMBER              ,
    RETROOMEGASECTIONNUMBER             ,
    LE                                  ,
    A_R                                 ,
    LE_HOP 
)
SELECT
    STG.OMEGA_TREATY_NUMBER                 ,
    STG.OMEGA_SECTION                       ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    STG.GROSS_ASSUMED_OMEGA_SECTION         ,
    NULL                                    ,
    STG.AOCSTEP                             ,
    STG.SENSITIVITYTYPE                     ,
    STG.SENSITIVITYVALUE                    ,
    STG.POLICYUWY                           ,
    NULL                                    ,
    STG.CURRENCY                            ,
    STG.PRODUCT                             ,
    STG.REPORTINGBASIS                      ,
    '''||P_SOURCE||'''          AS CREATED_BY,
    CAST(sysdate AS TIMESTAMP)  AS CREATED_DATE,
    ERR.REQUEST_ID              AS REQUEST_ID,
	ERR.ERROR_MESSAGE_ID        AS ERROR_MESSAGE_ID,  
    TO_DATE(STG.CLOSINGDATE,''DD/MM/YYYY'') AS CLOSING_DATE,
    STG.BUSINESSMATURITY                    ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL       
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
INNER JOIN DELIVERY_<env>.UPLOAD_ERROR_LOG ERR ON (ERR.ERROR_ROW = STG.LINE_NUMBER AND ERR.REQUEST_ID = ' || P_REQUEST_ID || ')
GROUP BY 
    STG.OMEGA_TREATY_NUMBER                 ,
    STG.OMEGA_SECTION                       ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    STG.GROSS_ASSUMED_OMEGA_SECTION         ,
    STG.AOCSTEP                             ,
    STG.SENSITIVITYTYPE                     ,
    STG.SENSITIVITYVALUE                    ,
    STG.POLICYUWY                           ,
    STG.CURRENCY                            ,
    STG.PRODUCT                             ,
    STG.REPORTINGBASIS                      ,
    '''||P_SOURCE||'''          ,
    CAST(sysdate AS TIMESTAMP)  ,
    ERR.REQUEST_ID              ,
	ERR.ERROR_MESSAGE_ID        ,  
    TO_DATE(STG.CLOSINGDATE,''DD/MM/YYYY'') ,
    STG.BUSINESSMATURITY                          
';    

V_INSERT_QUERY_CAN :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    OMEGA_TREATY_NUMBER                 ,
    OMEGA_SECTION                       ,
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    GROSS_ASSUMED_OMEGA_SECTION         ,
    SPLIT                               ,
    AOC_STEP                            ,
    SENSITIVITY_TYPE                    ,
    SENSITIVITY_VALUE                   ,
    POLICY_UWY                          ,
    POSITION                            ,
    CURRENCY                            ,
    PRODUCT                             ,
    BASIS                               ,
    CREATED_BY                          ,
    CREATED_DATE                        ,
    REQUEST_ID                          ,
    ERROR_MESSAGE_ID                    ,
    CLOSING_DATE                        ,
    BUSINESS_MATURITY                   ,
    RETROOMEGATREATYNUMBER              ,
    RETROOMEGASECTIONNUMBER             ,
    LE                                  ,
    A_R                                 ,
    LE_HOP 
)
SELECT
    STG.OMEGA_TREATY_NUMBER                 ,
    STG.OMEGA_SECTION                       ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_SECTION  ,
    NULL                                    ,
    STG.AOC_STEP                            ,
    STG.SENSITIVITY_TYPE                    ,
    STG.SENSITIVITY_VALUE                   ,
    STG.POLICY_UWY                          ,
    STG.POSITION                            ,
    ''CAD'' AS CURRENCY                     ,
    NULL                                    ,
    STG.BASIS                               ,
    '''||P_SOURCE||'''          AS CREATED_BY,
    CAST(sysdate AS TIMESTAMP)  AS CREATED_DATE,
    ERR.REQUEST_ID              AS REQUEST_ID,
	ERR.ERROR_MESSAGE_ID        AS ERROR_MESSAGE_ID,  
    TO_DATE(STG.CLOSING_DATE,''DD/MM/YYYY'') AS CLOSING_DATE,
    STG.BUSINESS_MATURITY                   ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL       
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
INNER JOIN DELIVERY_<env>.UPLOAD_ERROR_LOG ERR ON (ERR.ERROR_ROW = STG.LINE_NUMBER AND ERR.REQUEST_ID = ' || P_REQUEST_ID || ')
GROUP BY 
    STG.OMEGA_TREATY_NUMBER                 ,
    STG.OMEGA_SECTION                       ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_SECTION  ,
    STG.AOC_STEP                            ,
    STG.SENSITIVITY_TYPE                    ,
    STG.SENSITIVITY_VALUE                   ,
    STG.POLICY_UWY                          ,
    STG.POSITION                            ,
    STG.BASIS                               ,
    '''||P_SOURCE||'''          ,
    CAST(sysdate AS TIMESTAMP)  ,
    ERR.REQUEST_ID              ,
	ERR.ERROR_MESSAGE_ID        ,  
    TO_DATE(STG.CLOSING_DATE,''DD/MM/YYYY'') ,
    STG.BUSINESS_MATURITY                   
';   

V_INSERT_QUERY_FRGE :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    OMEGA_TREATY_NUMBER                 ,
    OMEGA_SECTION                       ,
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    GROSS_ASSUMED_OMEGA_SECTION         ,
    SPLIT                               ,
    AOC_STEP                            ,
    SENSITIVITY_TYPE                    ,
    SENSITIVITY_VALUE                   ,
    POLICY_UWY                          ,
    POSITION                            ,
    CURRENCY                            ,
    PRODUCT                             ,
    BASIS                               ,
    CREATED_BY                          ,
    CREATED_DATE                        ,
    REQUEST_ID                          ,
    ERROR_MESSAGE_ID                    ,
    CLOSING_DATE                        ,
    BUSINESS_MATURITY                   ,
    RETROOMEGATREATYNUMBER              ,
    RETROOMEGASECTIONNUMBER             ,
    LE                                  ,
    A_R                                 ,
    LE_HOP 
)
SELECT
    STG.OMEGA_TREATY_NUMBER                 ,
    STG.OMEGA_SECTION                       ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    STG.GROSS_ASSUMED_OMEGA_SECTION         ,
    NULL                                    ,
    STG.AOC_STEP                            ,
    STG.SENSITIVITY_TYPE                    ,
    STG.SENSITIVITY_VALUE                   ,
    STG.POLICY_UWY                          ,
    STG.POSITION AS POSITION                ,
    STG.CURRENCY                            ,
    NULL                                    ,
    STG.REPORTING_BASIS_TYPE                ,
    '''||P_SOURCE||'''          AS CREATED_BY,
    CAST(sysdate AS TIMESTAMP)  AS CREATED_DATE,
    ERR.REQUEST_ID              AS REQUEST_ID,
	ERR.ERROR_MESSAGE_ID        AS ERROR_MESSAGE_ID,  
    TO_DATE(STG.CLOSING_DATE,''DD/MM/YYYY'') AS CLOSING_DATE,
    STG.BUSINESS_MATURITY                   ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL       
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
INNER JOIN DELIVERY_<env>.UPLOAD_ERROR_LOG ERR ON (ERR.ERROR_ROW = STG.LINE_NUMBER AND ERR.REQUEST_ID = ' || P_REQUEST_ID || ')
GROUP BY 
    STG.OMEGA_TREATY_NUMBER                 ,
    STG.OMEGA_SECTION                       ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    STG.GROSS_ASSUMED_OMEGA_SECTION         ,
    NULL                                    ,
    STG.AOC_STEP                            ,
    STG.SENSITIVITY_TYPE                    ,
    STG.SENSITIVITY_VALUE                   ,
    STG.POLICY_UWY                          ,
    STG.POSITION                            ,
    STG.CURRENCY                            ,
    STG.REPORTING_BASIS_TYPE                ,
    '''||P_SOURCE||'''          ,
    CAST(sysdate AS TIMESTAMP)  ,
    ERR.REQUEST_ID              ,
	ERR.ERROR_MESSAGE_ID        ,  
    TO_DATE(STG.CLOSING_DATE,''DD/MM/YYYY'') ,
    STG.BUSINESS_MATURITY                                   
';   

V_INSERT_QUERY_UK :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    OMEGA_TREATY_NUMBER                 ,
    OMEGA_SECTION                       ,
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    GROSS_ASSUMED_OMEGA_SECTION         ,
    SPLIT                               ,
    AOC_STEP                            ,
    SENSITIVITY_TYPE                    ,
    SENSITIVITY_VALUE                   ,
    POLICY_UWY                          ,
    POSITION                            ,
    CURRENCY                            ,
    PRODUCT                             ,
    BASIS                               ,
    CREATED_BY                          ,
    CREATED_DATE                        ,
    REQUEST_ID                          ,
    ERROR_MESSAGE_ID                    ,
    CLOSING_DATE                        ,
    BUSINESS_MATURITY                   ,
    RETROOMEGATREATYNUMBER              ,
    RETROOMEGASECTIONNUMBER             ,
    LE                                  ,
    A_R                                 ,
    LE_HOP 
)
SELECT
    STG.OMEGATREATYNUMBER                   ,
    STG.OMEGASECTION                        ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    STG.GROSS_ASSUMED_OMEGA_SECTION         ,
    NULL                                    ,
    STG.AOCSTEP                             ,
    STG.SENSITIVITY_TYPE                    ,
    STG.SENSITIVITY_VALUE                   ,
    STG.POLICY_UWY                          ,
    NULL                                    ,
    STG.CURRENCY                            ,
    STG.PRODUCT                             ,
    STG.REPORTING_BASIS                     ,
    '''||P_SOURCE||'''          AS CREATED_BY,
    CAST(sysdate AS TIMESTAMP)  AS CREATED_DATE,
    ERR.REQUEST_ID              AS REQUEST_ID,
	ERR.ERROR_MESSAGE_ID        AS ERROR_MESSAGE_ID,  
    TO_DATE(STG.CLOSING_DATE ,''DD/MM/YYYY'') AS CLOSING_DATE,
    STG.BUSINESS_MATURITY                   ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL       
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
INNER JOIN DELIVERY_<env>.UPLOAD_ERROR_LOG ERR ON (ERR.ERROR_ROW = STG.LINE_NUMBER AND ERR.REQUEST_ID = ' || P_REQUEST_ID || ')
GROUP BY 
    STG.OMEGATREATYNUMBER                   ,
    STG.OMEGASECTION                        ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    STG.GROSS_ASSUMED_OMEGA_SECTION         ,
    STG.AOCSTEP                             ,
    STG.SENSITIVITY_TYPE                    ,
    STG.SENSITIVITY_VALUE                   ,
    STG.POLICY_UWY                          ,
    STG.CURRENCY                            ,
    STG.PRODUCT                             ,
    STG.REPORTING_BASIS                     ,
    '''||P_SOURCE||'''                      ,
    CAST(sysdate AS TIMESTAMP)              ,
    ERR.REQUEST_ID                          ,
	ERR.ERROR_MESSAGE_ID                    ,  
    TO_DATE(STG.CLOSING_DATE,''DD/MM/YYYY''),
    STG.BUSINESS_MATURITY                        
';  

V_INSERT_QUERY_MU :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    OMEGA_TREATY_NUMBER                 ,
    OMEGA_SECTION                       ,
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    GROSS_ASSUMED_OMEGA_SECTION         ,
    SPLIT                               ,
    AOC_STEP                            ,
    SENSITIVITY_TYPE                    ,
    SENSITIVITY_VALUE                   ,
    POLICY_UWY                          ,
    POSITION                            ,
    CURRENCY                            ,
    PRODUCT                             ,
    BASIS                               ,
    CREATED_BY                          ,
    CREATED_DATE                        ,
    REQUEST_ID                          ,
    ERROR_MESSAGE_ID                    ,
    CLOSING_DATE                        ,
    BUSINESS_MATURITY                   ,
    RETROOMEGATREATYNUMBER              ,
    RETROOMEGASECTIONNUMBER             ,
    LE                                  ,
    A_R                                 ,
    LE_HOP 
)
SELECT
    STG.OMEGA_TREATY_NUMBER                  ,
    STG.OMEGA_SECTION                        ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER    ,
    STG.GROSS_ASSUMED_OMEGA_SECTION          ,
    STG.SPLIT                                ,
    STG.AOC_STEP                             ,
    STG.SENSITIVITY_TYPE                     ,
    STG.SENSITIVITY_VALUE                    ,
    STG.POLICY_UWY                           ,
    STG.POSITION                             ,
    STG.CURRENCY                             ,
    NULL                                     ,
    STG.BASIS                                , 
    '''||P_SOURCE||'''          AS CREATED_BY,
    CAST(sysdate AS TIMESTAMP)  AS CREATED_DATE,
    ERR.REQUEST_ID              AS REQUEST_ID,
	ERR.ERROR_MESSAGE_ID        AS ERROR_MESSAGE_ID,  
    TO_DATE(STG.CLOSING_DATE ,''DD/MM/YYYY'') AS CLOSING_DATE,
    STG.BUSINESS_MATURITY                   ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL                                    ,
    NULL       
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
INNER JOIN DELIVERY_<env>.UPLOAD_ERROR_LOG ERR ON (ERR.ERROR_ROW = STG.LINE_NUMBER AND ERR.REQUEST_ID = ' || P_REQUEST_ID || ')
GROUP BY 
    STG.OMEGA_TREATY_NUMBER                  ,
    STG.OMEGA_SECTION                        ,
    STG.GROSS_ASSUMED_OMEGA_TREATY_NUMBER    ,
    STG.GROSS_ASSUMED_OMEGA_SECTION          ,
    STG.SPLIT                                ,
    STG.AOC_STEP                             ,
    STG.SENSITIVITY_TYPE                     ,
    STG.SENSITIVITY_VALUE                    ,
    STG.POLICY_UWY                           ,
    STG.POSITION                             ,
    STG.CURRENCY                             ,
    STG.BASIS                                , 
    '''||P_SOURCE||'''          ,
    CAST(sysdate AS TIMESTAMP)  ,
    ERR.REQUEST_ID              ,
	ERR.ERROR_MESSAGE_ID        ,  
    TO_DATE(STG.CLOSING_DATE ,''DD/MM/YYYY''),
    STG.BUSINESS_MATURITY                    
';    

V_INSERT_QUERY_LRM :=  'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(
    OMEGA_TREATY_NUMBER                 ,
    OMEGA_SECTION                       ,
    GROSS_ASSUMED_OMEGA_TREATY_NUMBER   ,
    GROSS_ASSUMED_OMEGA_SECTION         ,
    SPLIT                               ,
    AOC_STEP                            ,
    SENSITIVITY_TYPE                    ,
    SENSITIVITY_VALUE                   ,
    POLICY_UWY                          ,
    POSITION                            ,
    CURRENCY                            ,
    PRODUCT                             ,
    BASIS                               ,
    CREATED_BY                          ,
    CREATED_DATE                        ,
    REQUEST_ID                          ,
    ERROR_MESSAGE_ID                    ,
    CLOSING_DATE                        ,
    BUSINESS_MATURITY                   ,
    RETROOMEGATREATYNUMBER              ,
    RETROOMEGASECTIONNUMBER             ,
    LE                                  ,
    A_R                                 ,
    LE_HOP 
)
SELECT
    STG.TREATY_NUMBER                   ,
    STG.SECTION_NUMBER                  ,
    STG.GROSS_ASSUMED_CONTRACT_NUMBER   ,
    STG.GROSS_ASSUMED_SECTION_NUMBER    ,
    NULL                                ,
    STG.LEVEL_OF_ANALYSIS_CODE          ,
    ''''                                ,
    NULL                                ,
    STG.POLICY_UNDERWRITING_YEAR        ,
    STG.CSM_CASHFLOW_LEGS_CODE          ,
    STG.CURRENCY_CODE                   ,
    NULL                                ,
    STG.REPORTING_BASIS_CODE            , 
    '''||P_SOURCE||'''          AS CREATED_BY,
    CAST(sysdate AS TIMESTAMP)  AS CREATED_DATE,
    ERR.REQUEST_ID              AS REQUEST_ID,
	ERR.ERROR_MESSAGE_ID        AS ERROR_MESSAGE_ID,  
    TO_DATE(STG.CLOSING_DATE ,''DD/MM/YYYY'') AS CLOSING_DATE,
    STG.BUSINESS_MATURITY_CODE          ,
    NULL                                ,
    NULL                                ,
    NULL                                ,
    NULL                                ,
    NULL       
FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' STG
INNER JOIN DELIVERY_<env>.UPLOAD_ERROR_LOG ERR ON (ERR.ERROR_ROW = STG.LINE_NUMBER AND ERR.REQUEST_ID = ' || P_REQUEST_ID || ')
GROUP BY 
    STG.TREATY_NUMBER                   ,
    STG.SECTION_NUMBER                  ,
    STG.GROSS_ASSUMED_CONTRACT_NUMBER   ,
    STG.GROSS_ASSUMED_SECTION_NUMBER    ,
    STG.LEVEL_OF_ANALYSIS_CODE          ,
    STG.POLICY_UNDERWRITING_YEAR        ,
    STG.CSM_CASHFLOW_LEGS_CODE          ,
    STG.CURRENCY_CODE                   ,
    STG.REPORTING_BASIS_CODE            , 
    '''||P_SOURCE||'''                  ,
    CAST(sysdate AS TIMESTAMP)          ,
    ERR.REQUEST_ID                      ,
	ERR.ERROR_MESSAGE_ID                ,  
    TO_DATE(STG.CLOSING_DATE ,''DD/MM/YYYY''),
    STG.BUSINESS_MATURITY_CODE                    
';    

IF P_SOURCE = 'US PROPHET' THEN
	RAISE NOTICE 'Executing V_INSERT_QUERY_US: ''%''',V_INSERT_QUERY_US; 
	EXECUTE IMMEDIATE V_INSERT_QUERY_US; 
    
ELSIF P_SOURCE = 'APAC PROPHET' THEN   
	RAISE NOTICE 'Executing V_INSERT_QUERY_APAC: ''%''',V_INSERT_QUERY_APAC; 
	EXECUTE IMMEDIATE V_INSERT_QUERY_APAC; 

ELSIF P_SOURCE = 'UK PROPHET' THEN   
	RAISE NOTICE 'Executing V_INSERT_QUERY_UK: ''%''',V_INSERT_QUERY_UK; 
	EXECUTE IMMEDIATE V_INSERT_QUERY_UK; 
    
ELSIF P_SOURCE = 'CANADA_AXIS' THEN   
	RAISE NOTICE 'Executing V_INSERT_QUERY_CAN: ''%''',V_INSERT_QUERY_CAN; 
	EXECUTE IMMEDIATE V_INSERT_QUERY_CAN; 
    
ELSIF P_SOURCE = 'FRANCE_MMIND' THEN   
	RAISE NOTICE 'Executing V_INSERT_QUERY_FRGE: ''%''',V_INSERT_QUERY_FRGE; 
	EXECUTE IMMEDIATE V_INSERT_QUERY_FRGE; 

ELSIF P_SOURCE = 'GERMANY_MMIND' THEN   
	RAISE NOTICE 'Executing V_INSERT_QUERY_FRGE: ''%''',V_INSERT_QUERY_FRGE; 
	EXECUTE IMMEDIATE V_INSERT_QUERY_FRGE; 
 
ELSIF P_SOURCE = 'MANUAL UPLOAD' THEN   
	RAISE NOTICE 'Executing V_INSERT_QUERY_MU: ''%''',V_INSERT_QUERY_MU; 
	EXECUTE IMMEDIATE V_INSERT_QUERY_MU;    

ELSIF P_SOURCE = 'LRM' THEN   
	RAISE NOTICE 'Executing V_INSERT_QUERY_LRM: ''%''',V_INSERT_QUERY_LRM; 
	EXECUTE IMMEDIATE V_INSERT_QUERY_LRM;  
END IF;

EXCEPTION WHEN OTHERS THEN L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;