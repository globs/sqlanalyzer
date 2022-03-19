SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_ERROR_STAGING_APAC_PROJECTION;

CREATE OR REPLACE PROCEDURE SP_LOAD_ERROR_STAGING_APAC_PROJECTION(BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64)) 
RETURNS INTEGER
language nzplsql
AS
BEGIN_PROC

DECLARE
    P_REQUEST_ID        ALIAS FOR $1;
    P_SRC_SCHEMA        ALIAS FOR $2;
    P_SRC_TABLE         ALIAS FOR $3;
    P_TRG_SCHEMA        ALIAS FOR $4; 
    P_TRG_TABLE         ALIAS FOR $5; 
    P_EXCLUDE_LIST      VARCHAR(32000);
    P_EXCLUDE_LIST_1    VARCHAR(32000);
    L_ERR_CD            CHAR(5); 
    L_ERR_MSG           VARCHAR(32000); 
    V_REC               RECORD; 
    V_TOTALCOLS         BIGINT; 
    V_COLCOUNTER        BIGINT; 
    V_CURRENT_COLNAME   VARCHAR(100);
    V_CURRENT_COLNUM    VARCHAR(100);
    V_DEL_EXCLUDE_LIST  VARCHAR(32000);
    V_DEL_QUERY         VARCHAR(32000); 
    V_INS_QUERY         VARCHAR(32000);   
    V_COLS_QUERY        VARCHAR(32000);
    
BEGIN 

SET ISOLATION TO UR; 

V_DEL_QUERY = 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '  WHERE REQUEST_ID = ' || P_REQUEST_ID;

EXECUTE IMMEDIATE V_DEL_QUERY;

RAISE NOTICE 'DELETING TARGET TABLE'; 
RAISE NOTICE 'Executing V_DEL_QUERY: ''%''',V_DEL_QUERY; 

RAISE NOTICE 'Mandatory Check'; 

    V_TOTALCOLS = 0; 
    V_COLCOUNTER = 1; 
    P_EXCLUDE_LIST = '''JOINKEY'',''GROSS_ASSUMED_OMEGA_TREATY_NUMBER'',''GROSS_ASSUMED_OMEGA_SECTION'',''RUNNUMBER'',''SPCODE'',''POLICYUWY'',''NEWBUSINESS'',''PERIOD''';

    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLS IF EXISTS';
    CREATE TEMP TABLE SESSION.COLS (COLNAME VARCHAR(1000), COLNUMBER SMALLINT) DISTRIBUTE ON RANDOM;

    V_COLS_QUERY = 'INSERT INTO SESSION.COLS (COLNAME, COLNUMBER)
    SELECT 
        c.colname as COLNAME,
        c.colno+1 as COLNUMBER
    FROM 
        SYSCAT.COLUMNS C                  
        LEFT OUTER JOIN (
            SELECT 
                X.TABSCHEMA,
                X.TABNAME,
                COLNAME,
                TYPE 
            FROM 
                SYSCAT.TABCONST X 
                INNER JOIN 
                    SYSCAT.KEYCOLUSE Y 
                    ON X.TABSCHEMA = Y.TABSCHEMA 
                    AND X.TABNAME = Y.TABNAME 
                    AND X.CONSTNAME = Y.CONSTNAME 
            WHERE 
                X.TYPE = ''P'' 
                AND X.TABSCHEMA = ''' || P_SRC_SCHEMA || '''  
                AND X.TABNAME = ''' || P_SRC_TABLE || ''' ) U 
            ON U.TABSCHEMA = C.TABSCHEMA 
            AND U.TABNAME = C.TABNAME 
            AND U.COLNAME = C.COLNAME 
    WHERE 
        C.TABSCHEMA = ''' || P_SRC_SCHEMA || ''' 
        AND C.TABNAME = ''' || P_SRC_TABLE || '''
        AND C.COLNAME NOT IN (''REQUEST_ID'',''LINE_NUMBER'',''RANDOM_DISTRIBUTION_KEY'')
    ORDER BY 
        C.COLNO WITH UR';

    EXECUTE IMMEDIATE V_COLS_QUERY;

    SELECT COUNT(*) INTO V_TOTALCOLS FROM SESSION.COLS;

    V_DEL_EXCLUDE_LIST = 'DELETE FROM SESSION.COLS WHERE COLNAME IN (' || P_EXCLUDE_LIST || ')';

    EXECUTE IMMEDIATE V_DEL_EXCLUDE_LIST;
          
        WHILE V_COLCOUNTER <= V_TOTALCOLS LOOP
        
            IF (SELECT COUNT(*) FROM SESSION.COLS WHERE COLNUMBER = V_COLCOUNTER) = 1 THEN 
            
                SELECT COLNAME INTO V_CURRENT_COLNAME FROM SESSION.COLS WHERE COLNUMBER = V_COLCOUNTER;
                 
                V_INS_QUERY = 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '     (
                    LINE_NUMBER,
                    COL_NUMBER,
                    COL_VALUE,
                    ERROR_MESSAGE_ID,
                    REQUEST_ID
                )
                SELECT 
                       LINE_NUMBER, 
                       ' || V_COLCOUNTER || ' , 
                       ''' || V_CURRENT_COLNAME || ''', 
                       2,
                       ' || P_REQUEST_ID || '    
                FROM  ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' 
                WHERE  TRIM(' || V_CURRENT_COLNAME || ') = '''' OR ' || V_CURRENT_COLNAME || ' IS NULL';
               
                RAISE NOTICE 'LOADING TARGET TABLE for MANDATORY'; 
                RAISE NOTICE 'Executing V_INS_QUERY: ''%''',V_INS_QUERY; 
               
                EXECUTE IMMEDIATE V_INS_QUERY;
            
            END IF;
            
            V_COLCOUNTER = V_COLCOUNTER + 1;
            
        END LOOP; 
       
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLS IF EXISTS';
    EXECUTE IMMEDIATE 'COMMIT';

RAISE NOTICE 'Length Check'; 

    V_TOTALCOLS = 0; 
    V_COLCOUNTER = 1; 

    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLS3 IF EXISTS';
    CREATE TEMP TABLE SESSION.COLS3 (COLNAME VARCHAR(1000), COLNUMBER SMALLINT) DISTRIBUTE ON RANDOM;

    V_COLS_QUERY = 'INSERT INTO SESSION.COLS3 (COLNAME, COLNUMBER)
    SELECT 
        c.colname as COLNAME,
        c.colno+1 as COLNUMBER
    FROM 
        SYSCAT.COLUMNS C                  
        LEFT OUTER JOIN (
            SELECT 
                X.TABSCHEMA,
                X.TABNAME,
                COLNAME,
                TYPE 
            FROM 
                SYSCAT.TABCONST X 
                INNER JOIN 
                    SYSCAT.KEYCOLUSE Y 
                    ON X.TABSCHEMA = Y.TABSCHEMA 
                    AND X.TABNAME = Y.TABNAME 
                    AND X.CONSTNAME = Y.CONSTNAME 
            WHERE 
                X.TYPE = ''P'' 
                AND X.TABSCHEMA = ''' || P_SRC_SCHEMA || '''  
                AND X.TABNAME = ''' || P_SRC_TABLE || ''' ) U 
            ON U.TABSCHEMA = C.TABSCHEMA 
            AND U.TABNAME = C.TABNAME 
            AND U.COLNAME = C.COLNAME 
    WHERE 
        C.TABSCHEMA = ''' || P_SRC_SCHEMA || ''' 
        AND C.TABNAME = ''' || P_SRC_TABLE || '''
        AND C.COLNAME NOT IN (''REQUEST_ID'',''LINE_NUMBER'',''RANDOM_DISTRIBUTION_KEY'')
    ORDER BY 
        C.COLNO WITH UR';

    EXECUTE IMMEDIATE V_COLS_QUERY;

    SELECT COUNT(*) INTO V_TOTALCOLS FROM SESSION.COLS3;

    P_EXCLUDE_LIST_1 ='''RUNNUMBER'',''SENSITIVITYVALUE'',''CLOSINGDATE'',''P_PERIOD'',''P_TIME'',''PREM_WRITTEN'',''CLAIMS_INCURRED'',''CLAIMS_FROM_IBNP'',''COLLATERAL_COST'',''TOT_COMM'',
    ''FIN_COMM'',''FIN_BROK_FEE'',''RETRO_OVERRIDE_COMM'',''PS_TOTAL_ALLOCATION'',''DEP_MATH_RES_IF'',''DEP_UNEARN_PREM_RES_IF'',''DEP_INC_RISK_RES_IF'',''NDEP_MATH_RES_IF'',
    ''NDEP_UNEARN_PREM_RES_IF'',''NDEP_INC_RISK_RES_IF'',''DEP_IBNP_RES_IF'',''NDEP_IBNP_RES_IF'',''DAC_IF'',''RES_DEPINT'',''RES_NONDEPINT'',''TOT_REN_EXP'',''TOT_ACQ_EXP'',
    ''TOT_INV_EXP'',''TAX'',''SUM_ASSD_IF'',''VOBA'',''PV_PS_TOTAL_ALLOCATION'',''PREM_TAX'',''PREM_REFUND'',''INIT_COMM'',''COMM_CLAWBACK'',''REN_COMM'',''REN_EXP_ATTRIBUTABLE'',
    ''ACQ_EXP_ATTRIBUTABLE'',''INV_EXP_ATTRIBUTABLE'',''CLAIM_EXP_ATTRIBUTABLE'',''REN_EXP_NONATTRIBUTABLE'',''ACQ_EXP_NONATTRIBUTABLE'',''INV_EXP_NONATTRIBUTABLE'',''CLAIM_EXP_NONATTRIBUTABLE'',''COVERAGE_UNITS'',''LINE_NB''';

    V_DEL_EXCLUDE_LIST = 'DELETE FROM SESSION.COLS3 WHERE COLNAME IN (' || P_EXCLUDE_LIST_1 || ')';

    EXECUTE IMMEDIATE V_DEL_EXCLUDE_LIST;

          
        WHILE V_COLCOUNTER <= V_TOTALCOLS LOOP
        
            IF (SELECT COUNT(*) FROM SESSION.COLS3 WHERE COLNUMBER = V_COLCOUNTER) = 1 THEN 
            
                SELECT COLNAME INTO V_CURRENT_COLNAME FROM SESSION.COLS3 WHERE COLNUMBER = V_COLCOUNTER;
                 
                V_INS_QUERY = 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '     (
                    LINE_NUMBER,
                    COL_NUMBER,
                    COL_VALUE,
                    ERROR_MESSAGE_ID,
                    REQUEST_ID
                )
                SELECT 
                    LINE_NUMBER,
                    COL_NUMBER,
                    COL_VALUE,
                    ERROR_MESSAGE_ID,
                    REQUEST_ID
                FROM 
                    (SELECT 
                        CASE WHEN ''' || V_CURRENT_COLNAME || ''' = ''JOINKEY'' AND TRIM (' || V_CURRENT_COLNAME || ') <> '''' AND LENGTH(' || V_CURRENT_COLNAME || ')>27 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''PRODUCT'' AND LENGTH(' || V_CURRENT_COLNAME || ')>22 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''OMEGA_TREATY_NUMBER'' AND LENGTH(' || V_CURRENT_COLNAME || ')<> 9 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''OMEGA_SECTION''AND LENGTH(' || V_CURRENT_COLNAME || ')>2 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''GROSS_ASSUMED_OMEGA_TREATY_NUMBER'' AND TRIM (' || V_CURRENT_COLNAME || ') <> '''' AND LENGTH(' || V_CURRENT_COLNAME || ')> 9 THEN 1                
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''GROSS_ASSUMED_OMEGA_SECTION'' AND TRIM (' || V_CURRENT_COLNAME || ') <> '''' AND LENGTH(' || V_CURRENT_COLNAME || ')>2 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''AOCSTEP'' AND LENGTH(' || V_CURRENT_COLNAME || ')>100 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''SENSITIVITYTYPE'' AND LENGTH(' || V_CURRENT_COLNAME || ')>100 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''SPCODE'' AND TRIM (' || V_CURRENT_COLNAME || ') <> '''' AND LENGTH(' || V_CURRENT_COLNAME || ')>4 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''POLICYUWY'' AND TRIM (' || V_CURRENT_COLNAME || ') <> '''' AND LENGTH(' || V_CURRENT_COLNAME || ')>4 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''BUSINESSMATURITY'' AND LENGTH(' || V_CURRENT_COLNAME || ')>50 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''NEWBUSINESS'' AND TRIM (' || V_CURRENT_COLNAME || ') <> '''' AND LENGTH(' || V_CURRENT_COLNAME || ')>2 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''REPORTINGBASIS'' AND LENGTH(' || V_CURRENT_COLNAME || ')>50 THEN 1
                                 WHEN ''' || V_CURRENT_COLNAME || ''' = ''CURRENCY'' AND LENGTH(' || V_CURRENT_COLNAME || ')<> 3 THEN 1
                            ELSE 0 END AS CALC,
                           LINE_NUMBER AS LINE_NUMBER, 
                           ' || V_COLCOUNTER || ' AS COL_NUMBER, 
                           ''' || V_CURRENT_COLNAME || ''' AS COL_VALUE, 
                           6 AS ERROR_MESSAGE_ID,
                           ' || P_REQUEST_ID || ' AS REQUEST_ID    
                    FROM  ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' 
                    WHERE CALC=1
                    )
                ';
            
                RAISE NOTICE 'LOADING TARGET TABLE for LENGTH'; 
                RAISE NOTICE 'Executing V_INS_QUERY: ''%''',V_INS_QUERY; 
               
                EXECUTE IMMEDIATE V_INS_QUERY;
            
            END IF;
            
            V_COLCOUNTER = V_COLCOUNTER + 1;
            
        END LOOP; 
       
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLS3 IF EXISTS';
    EXECUTE IMMEDIATE 'COMMIT';
    
RAISE NOTICE 'Data type Check'; 

    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLSA IF EXISTS';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLSD IF EXISTS';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLSN IF EXISTS';
    CREATE TEMP TABLE SESSION.COLSA (COLNAME VARCHAR(1000), COLNUM SMALLINT, COLNUMBER SMALLINT,COLTYPE VARCHAR(1000)) DISTRIBUTE ON RANDOM;
    CREATE TEMP TABLE SESSION.COLSD (COLNAME VARCHAR(1000), COLNUM SMALLINT, COLNUMBER SMALLINT,COLTYPE VARCHAR(1000)) DISTRIBUTE ON RANDOM;
    CREATE TEMP TABLE SESSION.COLSN (COLNAME VARCHAR(1000), COLNUM SMALLINT, COLNUMBER SMALLINT,COLTYPE VARCHAR(1000)) DISTRIBUTE ON RANDOM;

    V_COLS_QUERY = 'INSERT INTO SESSION.COLSA (COLNAME,COLNUM, COLNUMBER,COLTYPE)
    SELECT 
        c.colname as COLNAME,
		c.colno+1 as COLNUM,
        row_number() over (order by colno) AS COLNUMBER,
        CASE 
            WHEN c.colname IN (''JOINKEY'',''PRODUCT'',''OMEGA_TREATY_NUMBER'',''GROSS_ASSUMED_OMEGA_TREATY_NUMBER'',''AOCSTEP'',''SENSITIVITYTYPE'',''BUSINESSMATURITY'',''REPORTINGBASIS'',''CURRENCY'') THEN ''ALPHANUMERIC''
            WHEN c.colname IN (''CLOSINGDATE'',''P_TIME'') THEN ''DATE''
            ELSE ''NUMERIC''
        END AS COLTYPE
    FROM 
        SYSCAT.COLUMNS C                  
        LEFT OUTER JOIN (
            SELECT 
                X.TABSCHEMA,
                X.TABNAME,
                COLNAME,
                TYPE 
            FROM 
                SYSCAT.TABCONST X 
                INNER JOIN 
                    SYSCAT.KEYCOLUSE Y 
                    ON X.TABSCHEMA = Y.TABSCHEMA 
                    AND X.TABNAME = Y.TABNAME 
                    AND X.CONSTNAME = Y.CONSTNAME 
            WHERE 
                X.TYPE = ''P'' 
                AND X.TABSCHEMA = ''' || P_SRC_SCHEMA || '''  
                AND X.TABNAME = ''' || P_SRC_TABLE || ''' ) U 
            ON U.TABSCHEMA = C.TABSCHEMA 
            AND U.TABNAME = C.TABNAME 
            AND U.COLNAME = C.COLNAME 
    WHERE 
        C.TABSCHEMA = ''' || P_SRC_SCHEMA || ''' 
        AND C.TABNAME = ''' || P_SRC_TABLE || '''
        AND C.COLNAME NOT IN (''REQUEST_ID'',''LINE_NUMBER'',''RANDOM_DISTRIBUTION_KEY'')
        AND COLTYPE= ''ALPHANUMERIC''
    ORDER BY 
        C.COLNO WITH UR';

    EXECUTE IMMEDIATE V_COLS_QUERY;

    V_COLS_QUERY = 'INSERT INTO SESSION.COLSD (COLNAME,COLNUM, COLNUMBER,COLTYPE)
    SELECT 
        c.colname as COLNAME,
		c.colno+1 as COLNUM,
        row_number() over (order by colno) AS COLNUMBER,
        CASE 
            WHEN c.colname IN (''JOINKEY'',''PRODUCT'',''OMEGA_TREATY_NUMBER'',''GROSS_ASSUMED_OMEGA_TREATY_NUMBER'',''AOCSTEP'',''SENSITIVITYTYPE'',''BUSINESSMATURITY'',''REPORTINGBASIS'',''CURRENCY'') THEN ''ALPHANUMERIC''
            WHEN c.colname IN (''CLOSINGDATE'',''P_TIME'') THEN ''DATE''
            ELSE ''NUMERIC''
        END AS COLTYPE
    FROM 
        SYSCAT.COLUMNS C                  
        LEFT OUTER JOIN (
            SELECT 
                X.TABSCHEMA,
                X.TABNAME,
                COLNAME,
                TYPE 
            FROM 
                SYSCAT.TABCONST X 
                INNER JOIN 
                    SYSCAT.KEYCOLUSE Y 
                    ON X.TABSCHEMA = Y.TABSCHEMA 
                    AND X.TABNAME = Y.TABNAME 
                    AND X.CONSTNAME = Y.CONSTNAME 
            WHERE 
                X.TYPE = ''P'' 
                AND X.TABSCHEMA = ''' || P_SRC_SCHEMA || '''  
                AND X.TABNAME = ''' || P_SRC_TABLE || ''' ) U 
            ON U.TABSCHEMA = C.TABSCHEMA 
            AND U.TABNAME = C.TABNAME 
            AND U.COLNAME = C.COLNAME 
    WHERE 
        C.TABSCHEMA = ''' || P_SRC_SCHEMA || ''' 
        AND C.TABNAME = ''' || P_SRC_TABLE || '''
        AND C.COLNAME NOT IN (''REQUEST_ID'',''LINE_NUMBER'',''RANDOM_DISTRIBUTION_KEY'')
        AND COLTYPE= ''DATE''
    ORDER BY 
        C.COLNO WITH UR';

    EXECUTE IMMEDIATE V_COLS_QUERY;
    
    V_COLS_QUERY = 'INSERT INTO SESSION.COLSN (COLNAME,COLNUM, COLNUMBER,COLTYPE)
    SELECT 
        c.colname as COLNAME,
		c.colno+1 as COLNUM,
        row_number() over (order by colno) AS COLNUMBER,
        CASE 
            WHEN c.colname IN (''JOINKEY'',''PRODUCT'',''OMEGA_TREATY_NUMBER'',''GROSS_ASSUMED_OMEGA_TREATY_NUMBER'',''AOCSTEP'',''SENSITIVITYTYPE'',''BUSINESSMATURITY'',''REPORTINGBASIS'',''CURRENCY'') THEN ''ALPHANUMERIC''
            WHEN c.colname IN (''CLOSINGDATE'',''P_TIME'') THEN ''DATE''
            ELSE ''NUMERIC''
        END AS COLTYPE
    FROM 
        SYSCAT.COLUMNS C                  
        LEFT OUTER JOIN (
            SELECT 
                X.TABSCHEMA,
                X.TABNAME,
                COLNAME,
                TYPE 
            FROM 
                SYSCAT.TABCONST X 
                INNER JOIN 
                    SYSCAT.KEYCOLUSE Y 
                    ON X.TABSCHEMA = Y.TABSCHEMA 
                    AND X.TABNAME = Y.TABNAME 
                    AND X.CONSTNAME = Y.CONSTNAME 
            WHERE 
                X.TYPE = ''P'' 
                AND X.TABSCHEMA = ''' || P_SRC_SCHEMA || '''  
                AND X.TABNAME = ''' || P_SRC_TABLE || ''' ) U 
            ON U.TABSCHEMA = C.TABSCHEMA 
            AND U.TABNAME = C.TABNAME 
            AND U.COLNAME = C.COLNAME 
    WHERE 
        C.TABSCHEMA = ''' || P_SRC_SCHEMA || ''' 
        AND C.TABNAME = ''' || P_SRC_TABLE || '''
        AND C.COLNAME NOT IN (''REQUEST_ID'',''LINE_NUMBER'',''RANDOM_DISTRIBUTION_KEY'')
        AND COLTYPE= ''NUMERIC''
    ORDER BY 
        C.COLNO WITH UR';

    EXECUTE IMMEDIATE V_COLS_QUERY;
    
    EXECUTE IMMEDIATE 'COMMIT';
    
    V_TOTALCOLS = 0; 
    V_COLCOUNTER = 1; 
    
    RAISE NOTICE 'Data type DATE Check'; 

    SELECT COUNT(*) INTO V_TOTALCOLS FROM SESSION.COLSD ;
    
    RAISE NOTICE 'Cols to check : ',V_TOTALCOLS; 
          
        WHILE V_COLCOUNTER <= V_TOTALCOLS LOOP
        
            IF (SELECT COUNT(*) FROM SESSION.COLSD WHERE COLNUMBER = V_COLCOUNTER) = 1 THEN 
            
                SELECT COLNAME INTO V_CURRENT_COLNAME FROM SESSION.COLSD WHERE COLNUMBER = V_COLCOUNTER;
				SELECT COLNUM INTO V_CURRENT_COLNUM FROM SESSION.COLSD WHERE COLNUMBER = V_COLCOUNTER;
                
                RAISE NOTICE 'Current col to check : ',V_CURRENT_COLNAME; 
                           
                V_INS_QUERY = 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
                    LINE_NUMBER,
                    COL_NUMBER,
                    COL_VALUE,
                    ERROR_MESSAGE_ID,
                    REQUEST_ID
                )
                SELECT                       
                       LINE_NUMBER AS LINE_NUMBER, 
                       ' || V_CURRENT_COLNUM || ' AS COL_NUMBER, 
                       ''' || V_CURRENT_COLNAME || ''' AS COL_VALUE, 
                       4 AS ERROR_MESSAGE_ID,
                       ' || P_REQUEST_ID || ' AS REQUEST_ID    
                FROM  ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' 
                WHERE REGEXP_LIKE(' || V_CURRENT_COLNAME || ' ,''^(0[1-9]|[12][0-9]|30|31)/(0[1-9]|1[0-2])/([0-9]{4})'') is FALSE
                ';
            
                RAISE NOTICE 'LOADING TARGET TABLE for DATE'; 
                RAISE NOTICE 'Executing V_INS_QUERY: ''%''',V_INS_QUERY; 
               
                EXECUTE IMMEDIATE V_INS_QUERY;
                    
            END IF;
            
            V_COLCOUNTER = V_COLCOUNTER + 1;
            
        END LOOP; 
        
    EXECUTE IMMEDIATE 'COMMIT';

    V_TOTALCOLS = 0; 
    V_COLCOUNTER = 1; 
    
    RAISE NOTICE 'Data type ALPHANUMERIC Check'; 

    SELECT COUNT(*) INTO V_TOTALCOLS FROM SESSION.COLSA ;
          
        WHILE V_COLCOUNTER <= V_TOTALCOLS LOOP
        
            IF (SELECT COUNT(*) FROM SESSION.COLSA WHERE COLNUMBER = V_COLCOUNTER) = 1 THEN 
            
                SELECT COLNAME INTO V_CURRENT_COLNAME FROM SESSION.COLSA WHERE COLNUMBER = V_COLCOUNTER;
				SELECT COLNUM INTO V_CURRENT_COLNUM FROM SESSION.COLSD WHERE COLNUMBER = V_COLCOUNTER;
                           
                V_INS_QUERY = 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '     (
                    LINE_NUMBER,
                    COL_NUMBER,
                    COL_VALUE,
                    ERROR_MESSAGE_ID,
                    REQUEST_ID
                )
                SELECT
                    LINE_NUMBER,
                    COL_NUMBER,
                    COL_VALUE,
                    ERROR_MESSAGE_ID,
                    REQUEST_ID            
                FROM 
                    (SELECT 
                           CASE WHEN ''' || V_CURRENT_COLNAME || ''' = ''JOINKEY'' AND LENGTH(' || V_CURRENT_COLNAME || ')>27 OR regexp_replace(' || V_CURRENT_COLNAME || ', ''[!@#$%^&*+='' || chr(34)|| '':_{}|<>\;,/?]'', ''~'') LIKE (''%~%'') AND regexp_replace(' || V_CURRENT_COLNAME || ', ''[!@#$%^&*+='' || chr(34)|| '':_{}<>\;,./?-]'', ''~'') LIKE (''%~%'') THEN 1      
                                WHEN ''' || V_CURRENT_COLNAME || ''' <> ''JOINKEY'' AND regexp_replace(' || V_CURRENT_COLNAME || ',''[!@#$%^&*+='' || chr(34)|| '':_{}|<>\;,/?]'',''~'') LIKE (''%~%'') THEN  1 
                           ELSE 0 END AS CALC,
                           LINE_NUMBER AS LINE_NUMBER, 
                           ' || V_CURRENT_COLNUM || ' AS COL_NUMBER, 
                           ''' || V_CURRENT_COLNAME || ''' AS COL_VALUE, 
                           4 AS ERROR_MESSAGE_ID,
                           ' || P_REQUEST_ID || ' AS REQUEST_ID    
                    FROM  ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' 
                    WHERE CALC=1 AND ''' || V_CURRENT_COLNAME || ''' <> ''AOCSTEP'')
                ';
            
                RAISE NOTICE 'LOADING TARGET TABLE for ALPHANUMERIC'; 
                RAISE NOTICE 'Executing V_INS_QUERY: ''%''',V_INS_QUERY; 
               
                EXECUTE IMMEDIATE V_INS_QUERY;
            
            END IF;
            
            V_COLCOUNTER = V_COLCOUNTER + 1;
            
        END LOOP; 
        
    EXECUTE IMMEDIATE 'COMMIT';    
    
    V_TOTALCOLS = 0; 
    V_COLCOUNTER = 1; 

    RAISE NOTICE 'Data type NUMERIC Check'; 
 
    SELECT COUNT(*) INTO V_TOTALCOLS FROM SESSION.COLSN ;
          
        WHILE V_COLCOUNTER <= V_TOTALCOLS LOOP
        
            IF (SELECT COUNT(*) FROM SESSION.COLSN WHERE COLNUMBER = V_COLCOUNTER) = 1 THEN 
            
                SELECT COLNAME INTO V_CURRENT_COLNAME FROM SESSION.COLSN WHERE COLNUMBER = V_COLCOUNTER;
				SELECT COLNUM INTO V_CURRENT_COLNUM FROM SESSION.COLSD WHERE COLNUMBER = V_COLCOUNTER;

                V_INS_QUERY = 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '     (
                    LINE_NUMBER,
                    COL_NUMBER,
                    COL_VALUE,
                    ERROR_MESSAGE_ID,
                    REQUEST_ID
                )
                SELECT
                    LINE_NUMBER,
                    COL_NUMBER,
                    COL_VALUE,
                    ERROR_MESSAGE_ID,
                    REQUEST_ID
                FROM
                    (SELECT 
                           CASE WHEN LENGTH(regexp_replace(' || V_CURRENT_COLNAME || ' ,''[-0-9]'','''')) > 1 OR regexp_replace(' || V_CURRENT_COLNAME || ',''[!@#$%^&*+='' || chr(34)|| '':_{}|<>\;,/?]'',''~'') LIKE (''%~%'') and REGEXP_LIKE(TRIM(' || V_CURRENT_COLNAME || ') ,''^\d+(\.\d*)?$'') != ''true'' THEN 1 ELSE 0 END AS CALC,
                           LINE_NUMBER AS LINE_NUMBER, 
                           ' || V_CURRENT_COLNUM || ' AS COL_NUMBER, 
                           ''' || V_CURRENT_COLNAME || ''' AS COL_VALUE, 
                           4 AS ERROR_MESSAGE_ID,
                           ' || P_REQUEST_ID || ' AS REQUEST_ID       
                    FROM  ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' 
                    WHERE  CALC=1)
               ';
                        
                RAISE NOTICE 'LOADING TARGET TABLE for NUMERIC'; 
                RAISE NOTICE 'Executing V_INS_QUERY: ''%''',V_INS_QUERY; 
               
                EXECUTE IMMEDIATE V_INS_QUERY;
            
            END IF;
            
            V_COLCOUNTER = V_COLCOUNTER + 1;
            
        END LOOP; 
        
    EXECUTE IMMEDIATE 'COMMIT';          
       
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLSA IF EXISTS';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLSD IF EXISTS';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.COLSN IF EXISTS';

EXCEPTION WHEN OTHERS THEN L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
    L_ERR_MSG := SQLERRM; 
    RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
    RETURN L_ERR_CD; 
    

END; 

END_PROC;

