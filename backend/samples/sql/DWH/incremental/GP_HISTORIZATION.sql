DROP PROCEDURE GP_HISTORIZATION;

CREATE OR REPLACE PROCEDURE GP_HISTORIZATION(BIGINT,
                                    CHARACTER VARYING(20),
                                    CHARACTER VARYING(64),
                                    CHARACTER VARYING(64),
                                    CHARACTER VARYING(64),
                                    CHARACTER VARYING(64),
                                    CHARACTER VARYING(64),
                                    CHARACTER VARYING(64),
                                    BOOLEAN) 
RETURNS INTEGER
language nzplsql
AS
BEGIN_PROC

DECLARE
    P_REQUEST_ID        ALIAS FOR $1;
    P_HIST_MODE         ALIAS FOR $2;
    P_SRC_SCHEMA        ALIAS FOR $3;
    P_SRC_TABLE         ALIAS FOR $4;
    P_TRG_SCHEMA        ALIAS FOR $5; 
    P_TRG_TABLE         ALIAS FOR $6; 
    P_DLV_SCHEMA        ALIAS FOR $7; 
    P_DLV_TABLE         ALIAS FOR $8; 
    P_SRC_PURGE			ALIAS FOR $9;
    L_ERR_CD            CHAR(5); 
    L_ERR_MSG           VARCHAR(32000); 
    V_REC               RECORD; 
    V_HASH_QUERY        VARCHAR(32000); 
    V_INSERT_WRK        VARCHAR(32000); 
    V_INS_UPD_WRK       VARCHAR(32000); 
    V_INS_CLO_UPD_WRK   VARCHAR(32000); 
    V_DELETE_UPD_TRG    VARCHAR(32000); 
    V_TO_DEL_FULL_WRK   VARCHAR(32000); 
    V_TO_DEL_INC_WRK    VARCHAR(32000); 
    V_DELETE_TRG        VARCHAR(32000); 
    V_INSERT_TRG        VARCHAR(32000); 
    V_GIVEN_TS          TIMESTAMP; 
    V_INFINITE_TS       TIMESTAMP; 
    V_SUPP_TS           TIMESTAMP; 
    V_SCOPE             VARCHAR(32); 
    V_CPT               BIGINT; 
    V_CHK_PK            BIGINT; 
    V_HASH_KEY          VARCHAR(1000); 
    V_PIVOT_KEY         VARCHAR(1000); 
    V_STG_TABLE         VARCHAR(1000); 
    V_WRK_TABLE         VARCHAR(1000);     
    V_DIST_KEY          VARCHAR(1000); 
    V_SRC_COL           VARCHAR(1000); 
    V_STG_COL           VARCHAR(1000); 
    V_TRG_COL           VARCHAR(1000); 
    V_ALL_COL           VARCHAR(1000); 
    V_STG_SELECT        VARCHAR(1000); 
    V_WRK_SELECT        VARCHAR(1000); 
    V_WRK_SEL_STG       VARCHAR(1000); 
    V_WRK_SEL_TRG       VARCHAR(1000); 
    V_WRK_SEL_SCP       VARCHAR(1000); 
    V_WRK_TBL_DDL       VARCHAR(1000); 
    V_STG_TBL_DDL       VARCHAR(1000); 
    V_SRC_TBL_DDL       VARCHAR(1000);     
    
BEGIN 

SET ISOLATION TO UR; 
V_CPT = 0; 
V_CHK_PK = 0; 
V_GIVEN_TS = CURRENT_TIMESTAMP; 
V_INFINITE_TS = '9999-12-31'; 
V_SUPP_TS = '9999-12-31'; 
V_STG_TABLE = P_TRG_TABLE || '_STG'; 
V_WRK_TABLE = P_TRG_TABLE || '_WRK'; 
V_DIST_KEY :='';
V_SRC_COL  :='';
V_STG_COL  :='';
V_TRG_COL  :='';
V_ALL_COL  :='';
V_WRK_SELECT :='';
V_STG_SELECT :=''; 
V_WRK_SEL_STG:='';
V_WRK_SEL_TRG:='';
V_WRK_SEL_SCP:='';
V_WRK_TBL_DDL:='';
V_STG_TBL_DDL:=''; 
V_SRC_TBL_DDL:=''; 

DROP TABLE SESSION.KEYS IF EXISTS; 

CREATE TEMP TABLE SESSION.KEYS (NAME VARCHAR(50), VALUE VARCHAR(1000)) DISTRIBUTE ON RANDOM;

EXECUTE IMMEDIATE 'INSERT INTO SESSION.KEYS  
	SELECT
		CFK.KEY_NAME AS NAME, 
    	''HASH(NVL(''||LISTAGG(
			CASE 
				WHEN C.TYPENAME = ''DATE'' THEN ''VARCHAR_FORMAT(''||CFK.COLUMN_NAME||'', ''''YYYYMMDD'''')'' 
				WHEN C.TYPENAME = ''TIMESTAMP'' THEN ''VARCHAR_FORMAT(''||CFK.COLUMN_NAME||'', ''''yyyymmddhh24miss'''')'' 
				ELSE CFK.COLUMN_NAME 
			END,'', ''''0'''')||''''-''''||NVL('') WITHIN GROUP(ORDER BY CFK.COLUMN_ORDER) ||'', ''''0''''), 2)'' AS VALUE 
	FROM ' || P_DLV_SCHEMA || '.' || P_DLV_TABLE || ' CFK
    INNER JOIN SYSCAT.COLUMNS C
			ON C.COLNAME = CFK.COLUMN_NAME
			AND C.TABSCHEMA = ''' || P_TRG_SCHEMA || ''' 
			AND C.TABNAME = ''' || P_TRG_TABLE || ''' 
			AND C.COLNAME <> ''RANDOM_DISTRIBUTION_KEY''
	WHERE 
		TABLE_NAME = ''' || P_TRG_TABLE || '''
	GROUP BY 
		CFK.KEY_NAME WITH UR';

SELECT VALUE INTO V_HASH_KEY FROM SESSION.KEYS WHERE NAME = 'HASH_KEY'; 
SELECT VALUE INTO V_PIVOT_KEY FROM SESSION.KEYS WHERE NAME = 'PIVOT_KEY'; 

EXECUTE IMMEDIATE 'INSERT INTO SESSION.KEYS  SELECT ''V_CPT'' AS NAME ,COUNT(0) FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' WHERE REQUEST_ID=' || P_REQUEST_ID; 
 
SELECT VALUE INTO V_CPT FROM SESSION.KEYS WHERE NAME = 'V_CPT'; 

IF V_CPT > 0 THEN RAISE NOTICE 'RECORDS ARE PRESENT IN STAGING FOR GIVEN REQUEST_ID'; 
    
    FOR V_REC IN 
    SELECT 
        C.COLNAME                                           AS ATTNAME,
        C.TYPENAME                                          AS FORMAT_TYPE,
        CASE U.TYPE WHEN 'P' THEN 'Y' ELSE 'N' END          AS ISPK_B, 
        CASE WHEN C.GENERATED <>'A' THEN 'Y' ELSE 'N' END   AS WRK_DDL,
        CASE WHEN C.GENERATED <>'A' AND  c.colname NOT IN ('VALID_FROM','VALID_TO','SUPP_DATE','CREATED_REQUEST_ID','MODIFIED_REQUEST_ID','DELETED_REQUEST_ID') THEN 'Y' ELSE 'N' END AS STG_DDL,
        CASE WHEN C.GENERATED <>'A' AND  c.colname NOT IN ('HASH_KEY','PIVOT_KEY','VALID_FROM','VALID_TO','SUPP_DATE','CREATED_REQUEST_ID','MODIFIED_REQUEST_ID','DELETED_REQUEST_ID') THEN 'Y' ELSE 'N' END AS SRC_DDL
    FROM SYSCAT.COLUMNS C                  
    LEFT OUTER JOIN (SELECT X.TABSCHEMA,X.TABNAME,COLNAME,TYPE 
            FROM SYSCAT.TABCONST X 
            INNER JOIN SYSCAT.KEYCOLUSE Y ON X.TABSCHEMA=Y.TABSCHEMA AND X.TABNAME=Y.TABNAME AND X.CONSTNAME=Y.CONSTNAME 
            WHERE X.TYPE='P' AND X.TABSCHEMA=P_TRG_SCHEMA AND X.TABNAME=P_TRG_TABLE ) U ON U.TABSCHEMA=C.TABSCHEMA AND U.TABNAME=C.TABNAME AND U.COLNAME=C.COLNAME 
    WHERE C.TABSCHEMA=P_TRG_SCHEMA AND C.TABNAME=P_TRG_TABLE AND C.COLNAME <> 'RANDOM_DISTRIBUTION_KEY' ORDER BY C.COLNO WITH UR; 
    
        RAISE NOTICE 'Treating column ''%'' ', V_REC.ATTNAME;  
               V_ALL_COL = V_ALL_COL || V_REC.ATTNAME || ',';  
            IF V_REC.ISPK_B = 'Y' THEN V_DIST_KEY = V_DIST_KEY || V_REC.ATTNAME || ','; END IF; 
            IF V_REC.WRK_DDL = 'Y' THEN V_WRK_TBL_DDL = V_WRK_TBL_DDL || V_REC.ATTNAME || ','; END IF;  
            IF V_REC.STG_DDL = 'Y' THEN V_STG_TBL_DDL = V_STG_TBL_DDL || V_REC.ATTNAME || ','; END IF; 
            IF V_REC.SRC_DDL = 'Y' THEN V_SRC_TBL_DDL = V_SRC_TBL_DDL || V_REC.ATTNAME || ','; END IF; 
    END LOOP; 
     
    V_DIST_KEY= SUBSTRING(V_DIST_KEY,0,LENGTH(V_DIST_KEY)); 
    V_SRC_COL = SUBSTRING(V_SRC_TBL_DDL,0,LENGTH(V_SRC_TBL_DDL)); 
    V_STG_COL = SUBSTRING(V_STG_TBL_DDL,0,LENGTH(V_STG_TBL_DDL)); 
    V_TRG_COL = SUBSTRING(V_WRK_TBL_DDL,0,LENGTH(V_WRK_TBL_DDL)); 
    V_ALL_COL = SUBSTRING(V_ALL_COL,0,LENGTH(V_ALL_COL)); 
    
    V_STG_SELECT = V_SRC_COL || ',HASH_KEY,PIVOT_KEY'; 
    V_WRK_TBL_DDL = V_STG_SELECT || ',CREATED_REQUEST_ID AS HISTORIZATION_MODE,VALID_FROM,VALID_TO,SUPP_DATE,CREATED_REQUEST_ID,MODIFIED_REQUEST_ID,DELETED_REQUEST_ID'; 
    V_WRK_SELECT = V_STG_SELECT ||',HISTORIZATION_MODE,VALID_FROM,VALID_TO,SUPP_DATE,CREATED_REQUEST_ID,MODIFIED_REQUEST_ID,DELETED_REQUEST_ID'; 
    V_WRK_SEL_STG = 'STG.'|| REGEXP_REPLACE(V_STG_SELECT,',',',STG.'); 
    V_WRK_SEL_TRG = REGEXP_REPLACE(V_WRK_SEL_STG,'STG.','TRG.'); 
    V_WRK_SEL_SCP = REGEXP_REPLACE(V_WRK_SEL_STG,'STG.','SCOPE.'); 
    
    EXECUTE IMMEDIATE 'DECLARE GLOBAL TEMPORARY TABLE SESSION.' || V_STG_TABLE || ' AS (SELECT ' || V_STG_SELECT || ' FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ') 
    WITH NO DATA ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE DISTRIBUTE ON RANDOM'; 
 
    V_HASH_QUERY = 'INSERT INTO SESSION.' || V_STG_TABLE || '(' || V_STG_SELECT || ') 
    SELECT ' || V_SRC_COL || ', 
    ' || V_HASH_KEY || '    AS HASH_KEY, 
    ' || V_PIVOT_KEY || '   AS PIVOT_KEY 
    FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' wrk               
    WHERE REQUEST_ID = ' || P_REQUEST_ID || ''; 
       
    RAISE NOTICE 'Executing V_HASH_QUERY: ''%''', V_HASH_QUERY; 
    EXECUTE IMMEDIATE V_HASH_QUERY; 
    EXECUTE IMMEDIATE 'INSERT INTO SESSION.KEYS SELECT ''V_CHK_PK'' AS NAME, MAX(t.VALUES) AS VALUE FROM (SELECT COUNT(0) AS VALUES FROM SESSION.' || V_STG_TABLE || ' GROUP BY HASH_KEY HAVING COUNT(0)>1) t'; 
    
    SELECT VALUE INTO V_CHK_PK FROM SESSION.KEYS WHERE NAME = 'V_CHK_PK'; 
       
    IF V_CHK_PK IS NULL  
        THEN RAISE NOTICE 'DATA UNICITY CONSISTENCY PASSED FOR STAGING DATA'; 
        EXECUTE IMMEDIATE 'DECLARE GLOBAL TEMPORARY TABLE SESSION.' || V_WRK_TABLE || ' 
        AS (SELECT ' || V_WRK_TBL_DDL || ' FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ')  
        WITH NO DATA ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE DISTRIBUTE ON HASH(' || V_DIST_KEY || ')'; 
        
        V_INSERT_WRK = 'INSERT INTO SESSION.' || V_WRK_TABLE || '(' || V_WRK_SELECT || ') 
        SELECT  ' || V_WRK_SEL_STG || ',    
                1                                   AS HISTORIZATION_MODE,    
                ''' || V_GIVEN_TS || '''            AS VALID_FROM,    
                ''' || V_INFINITE_TS || '''         AS VALID_TO  ,    
                ''' || V_SUPP_TS || '''             AS SUPP_DATE ,    
                ' || P_REQUEST_ID || '              AS CREATED_REQUEST_ID,    
                NULL                                AS MODIFIED_REQUEST_ID,    
                NULL                                AS DELETED_REQUEST_ID  
            FROM SESSION.' || V_STG_TABLE || '  AS STG 
            LEFT OUTER JOIN ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' AS TRG  ON (STG.HASH_KEY = TRG.HASH_KEY 
                AND DATE(TRG.VALID_TO) =DATE(''' || V_INFINITE_TS || ''') AND DATE(TRG.SUPP_DATE) =DATE(''' || V_SUPP_TS || ''')) 
            WHERE TRG.HASH_KEY IS NULL'; 
        
        V_INS_UPD_WRK = 'INSERT INTO SESSION.' || V_WRK_TABLE || '(' || V_WRK_SELECT || ')    
        SELECT  ' || V_WRK_SEL_STG || ',        
                2                                   AS HISTORIZATION_MODE,        
                ''' || V_GIVEN_TS || '''            AS VALID_FROM,        
                ''' || V_INFINITE_TS || '''         AS VALID_TO,        
                ''' || V_SUPP_TS || '''             AS SUPP_DATE,        
                TRG.CREATED_REQUEST_ID              AS CREATED_REQUEST_ID,        
                ' || P_REQUEST_ID || '              AS MODIFIED_REQUEST_ID,        
                NULL                                AS DELETED_REQUEST_ID    
            FROM SESSION.' || V_STG_TABLE || '  AS STG 
            INNER JOIN ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' AS TRG  ON (STG.HASH_KEY = TRG.HASH_KEY 
                AND DATE(TRG.VALID_TO) =DATE(''' || V_INFINITE_TS || ''') AND DATE(TRG.SUPP_DATE) =DATE(''' || V_SUPP_TS || ''')) 
            WHERE  TRG.PIVOT_KEY <> STG.PIVOT_KEY'; 
     
        V_INS_CLO_UPD_WRK = 'INSERT INTO SESSION.' || V_WRK_TABLE || '(' || V_WRK_SELECT || ')    
        SELECT  ' || V_WRK_SEL_TRG || ',
                3                                   AS HISTORIZATION_MODE,    
                TRG.VALID_FROM                      AS VALID_FROM,                             
                ''' || V_GIVEN_TS || '''            AS VALID_TO,        
                TRG.SUPP_DATE                       AS SUPP_DATE,        
                TRG.CREATED_REQUEST_ID              AS CREATED_REQUEST_ID,                              
                ' || P_REQUEST_ID || '              AS MODIFIED_REQUEST_ID,                              
                NULL                                AS DELETED_REQUEST_ID 
            FROM SESSION.' || V_STG_TABLE || '  AS STG 
            INNER JOIN ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' AS TRG ON (STG.HASH_KEY = TRG.HASH_KEY 
                AND DATE(TRG.VALID_TO) =DATE(''' || V_INFINITE_TS || ''') AND DATE(TRG.SUPP_DATE) =DATE(''' || V_SUPP_TS || '''))  
            WHERE  TRG.PIVOT_KEY <> STG.PIVOT_KEY'; 
 
        V_DELETE_UPD_TRG = 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' TRG 
        WHERE EXISTS (       SELECT 1         
            FROM SESSION.' || V_WRK_TABLE || ' WRK          
            WHERE WRK.HASH_KEY = TRG.HASH_KEY AND WRK.PIVOT_KEY = TRG.PIVOT_KEY AND DATE(TRG.VALID_TO) =DATE(''' || V_INFINITE_TS || ''') 
              AND DATE(TRG.SUPP_DATE) =DATE(''' || V_SUPP_TS || ''')  AND WRK.HISTORIZATION_MODE IN (1,2,3))'; 
   
        V_TO_DEL_FULL_WRK = 'INSERT INTO SESSION.' || V_WRK_TABLE || '(' || V_WRK_SELECT || ') 
        SELECT  ' || V_WRK_SEL_SCP || ',    
                4                                   AS HISTORIZATION_MODE,    
                SCOPE.VALID_FROM                    AS VALID_FROM,         
                SCOPE.VALID_TO                      AS VALID_TO,        
                ''' || V_GIVEN_TS || '''            AS SUPP_DATE,        
                SCOPE.CREATED_REQUEST_ID            AS CREATED_REQUEST_ID, 
                SCOPE.MODIFIED_REQUEST_ID           AS MODIFIED_REQUEST_ID,
                ' || P_REQUEST_ID || '              AS DELETED_REQUEST_ID 
            FROM SESSION.' || V_STG_TABLE || '  AS STG 
            RIGHT OUTER JOIN  '|| P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' AS SCOPE ON (STG.HASH_KEY = SCOPE.HASH_KEY) 
            WHERE STG.HASH_KEY IS NULL AND DATE(SCOPE.VALID_TO) =DATE(''' || V_INFINITE_TS || ''') AND DATE(SCOPE.SUPP_DATE) =DATE(''' || V_SUPP_TS || ''') '; 
  
        V_DELETE_TRG = 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' TRG 
        WHERE EXISTS (       SELECT 1         
            FROM SESSION.' || V_WRK_TABLE || ' WRK          
            WHERE WRK.HASH_KEY = TRG.HASH_KEY AND WRK.PIVOT_KEY = TRG.PIVOT_KEY AND DATE(TRG.VALID_TO) =DATE(''' || V_INFINITE_TS || ''') AND DATE(TRG.SUPP_DATE) =DATE(''' || V_SUPP_TS || ''') AND WRK.HISTORIZATION_MODE IN (4))'; 
 
        V_INSERT_TRG = 'INSERT INTO  ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (' || V_TRG_COL || ') 
        SELECT ' || V_TRG_COL || '    FROM SESSION.' || V_WRK_TABLE; 

        IF P_HIST_MODE = 'FULL' 
            THEN RAISE NOTICE 'FULL SCOPE TREATMENT'; 
                RAISE NOTICE 'Executing V_INSERT_WRK: ''%''',V_INSERT_WRK; 
                EXECUTE IMMEDIATE V_INSERT_WRK; 
                RAISE NOTICE 'Executing V_INS_UPD_WRK: ''%''',V_INS_UPD_WRK; 
                EXECUTE IMMEDIATE V_INS_UPD_WRK; 
                RAISE NOTICE 'Executing V_INS_CLO_UPD_WRK: ''%''',V_INS_CLO_UPD_WRK; 
                EXECUTE IMMEDIATE V_INS_CLO_UPD_WRK; 

                RAISE NOTICE 'Calculating statistics';
                EXECUTE IMMEDIATE 'CALL ADMIN_CMD(''RUNSTATS ON TABLE SESSION.' || V_WRK_TABLE || ' ON ALL COLUMNS AND COLUMNS ((HASH_KEY,PIVOT_KEY) ) WITH DISTRIBUTION ON ALL COLUMNS SET PROFILE'')';
                EXECUTE IMMEDIATE 'COMMIT';

                RAISE NOTICE 'Executing V_DELETE_UPD_TRG: ''%''',V_DELETE_UPD_TRG; 
                EXECUTE IMMEDIATE V_DELETE_UPD_TRG; 
                RAISE NOTICE 'Executing V_TO_DEL_FULL_WRK: ''%''',V_TO_DEL_FULL_WRK; 
                EXECUTE IMMEDIATE V_TO_DEL_FULL_WRK; 
                RAISE NOTICE 'Executing V_DELETE_TRG: ''%''',V_DELETE_TRG; 
                EXECUTE IMMEDIATE V_DELETE_TRG; 
                RAISE NOTICE 'Executing V_INSERT_TRG: ''%''',V_INSERT_TRG; 
                EXECUTE IMMEDIATE V_INSERT_TRG; 
        ELSE RAISE NOTICE 'INCREMENTAL SCOPE TREATMENT'; 
                RAISE NOTICE 'Executing V_INSERT_WRK: ''%''',V_INSERT_WRK; 
                EXECUTE IMMEDIATE V_INSERT_WRK; 
                RAISE NOTICE 'Executing V_INS_UPD_WRK: ''%''',V_INS_UPD_WRK; 
                EXECUTE IMMEDIATE V_INS_UPD_WRK; 
                RAISE NOTICE 'Executing V_INS_CLO_UPD_WRK: ''%''',V_INS_CLO_UPD_WRK; 
                EXECUTE IMMEDIATE V_INS_CLO_UPD_WRK; 

                RAISE NOTICE 'Calculating statistics';
                EXECUTE IMMEDIATE 'CALL ADMIN_CMD(''RUNSTATS ON TABLE SESSION.' || V_WRK_TABLE || ' ON ALL COLUMNS AND COLUMNS ((HASH_KEY,PIVOT_KEY) ) WITH DISTRIBUTION ON ALL COLUMNS SET PROFILE'')';
                EXECUTE IMMEDIATE 'COMMIT';

                RAISE NOTICE 'Executing V_DELETE_UPD_TRG: ''%''',V_DELETE_UPD_TRG; 
                EXECUTE IMMEDIATE V_DELETE_UPD_TRG; 
                RAISE NOTICE 'Executing V_INSERT_TRG: ''%''',V_INSERT_TRG; 
                EXECUTE IMMEDIATE V_INSERT_TRG; 
        END IF; 
        	
        IF P_SRC_PURGE is TRUE
		THEN EXECUTE IMMEDIATE 'DELETE FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' WHERE REQUEST_ID = ' || P_REQUEST_ID;
        END IF;
        
    ELSE RAISE EXCEPTION 'DATA UNICITY CONSISTENCY FAILED FOR STAGING DATA'; 
    END IF; 
    
    ELSE RAISE NOTICE 'RECORDS ARE NOT PRESENT IN STAGING FOR GIVEN REQUEST_ID'; 
END IF; 
	

EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_STG_TABLE || ' IF EXISTS'; 
EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TABLE || ' IF EXISTS'; 
DROP TABLE SESSION.KEYS IF EXISTS; 
 
EXCEPTION WHEN OTHERS THEN L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;
