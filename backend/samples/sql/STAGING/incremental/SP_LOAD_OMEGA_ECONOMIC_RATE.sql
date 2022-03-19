SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_OMEGA_ECONOMIC_RATE;

CREATE OR REPLACE PROCEDURE SP_LOAD_OMEGA_ECONOMIC_RATE(
	BIGINT,
	CHARACTER VARYING(64),
	CHARACTER VARYING(64)
) 
RETURNS INTEGER
language nzplsql
AS
BEGIN_PROC

DECLARE
	P_REQUEST_ID	ALIAS FOR $1;
    P_TRG_SCHEMA	ALIAS FOR $2;
    P_TRG_TABLE		ALIAS FOR $3;
	V_INS_YC		VARCHAR(ANY);
	V_INS_DR		VARCHAR(ANY);
	V_INS_IN		VARCHAR(ANY);
	V_INS_DEF		VARCHAR(ANY);
	V_INS_DWH		VARCHAR(ANY);
	V_WRK_TBL		VARCHAR(1000);
	V_WRK_TBL_2		VARCHAR(1000);
	V_HASH_KEY      VARCHAR(1000);
	V_PIVOT_KEY     VARCHAR(1000);
    V_SUPP_TS       TIMESTAMP;
	L_ERR_CD		CHAR(5);
	L_ERR_MSG		VARCHAR(32000);

BEGIN
	SET ISOLATION TO UR;
	
	V_WRK_TBL := P_TRG_TABLE || '_STG';
	V_WRK_TBL_2 := P_TRG_TABLE || '_STG_2';
	V_SUPP_TS = '9999-12-31'; 

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
		FROM DELIVERY_<env>.CONF_TABLE_KEY CFK
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
	
	EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TBL || ' IF EXISTS';
	EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TBL_2 || ' IF EXISTS';
	
	EXECUTE IMMEDIATE 'DECLARE GLOBAL TEMPORARY TABLE SESSION. ' || V_WRK_TBL || '(
		REPORTING_BASIS_ID SMALLINT,
		CLOSING_DATE TIMESTAMP NOT NULL,
		ORIGIN_CLOSING_DATE DATE,
		PARAMETER_TYPE_ID SMALLINT NOT NULL,
		SEGMENT_TYPE_ID SMALLINT,
		SEGMENT_CODE VARCHAR(16),
		SUBSIDIARY_CODE SMALLINT,
		SUBLEDGER_CODE SMALLINT,
		LOB_CODE CHAR(2),
		DOMAIN_CODE VARCHAR(16),
		ECONOMIC_DATA_AS_OF_DATE VARCHAR(16),
		BASE_CURRENCY_CODE CHAR(3),
		CURRENCY_CODE CHAR(3) DEFAULT ''ZZZ'' NOT NULL,
		MATURITY_TYPE_ID SMALLINT,
		MATURITY SMALLINT NOT NULL,
		USAGE_TYPE_ID SMALLINT,
		RATE DECFLOAT NOT NULL,
		DISCOUNT_FACTOR_MYCF DECFLOAT,
		SOURCE_IDENTIFIER VARCHAR(100),
		VALID_FROM TIMESTAMP NOT NULL,
		VALID_TO TIMESTAMP NOT NULL,
		REQUEST_ID BIGINT
	) ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE DISTRIBUTE ON RANDOM';

	EXECUTE IMMEDIATE 'DECLARE GLOBAL TEMPORARY TABLE SESSION. ' || V_WRK_TBL_2 || '(
		REPORTING_BASIS_ID SMALLINT,
		CLOSING_DATE TIMESTAMP NOT NULL,
		ORIGIN_CLOSING_DATE DATE,
		PARAMETER_TYPE_ID SMALLINT NOT NULL,
		SEGMENT_TYPE_ID SMALLINT,
		SEGMENT_CODE VARCHAR(16),
		SUBSIDIARY_CODE SMALLINT,
		SUBLEDGER_CODE SMALLINT,
		LOB_CODE CHAR(2),
		DOMAIN_CODE VARCHAR(16),
		ECONOMIC_DATA_AS_OF_DATE VARCHAR(16),
		BASE_CURRENCY_CODE CHAR(3),
		CURRENCY_CODE CHAR(3) DEFAULT ''ZZZ'' NOT NULL,
		MATURITY_TYPE_ID SMALLINT,
		MATURITY SMALLINT NOT NULL,
		USAGE_TYPE_ID SMALLINT,
		RATE DECFLOAT NOT NULL,
		DISCOUNT_FACTOR DECFLOAT,
		DISCOUNT_FACTOR_MYCF DECFLOAT,
		SOURCE_IDENTIFIER VARCHAR(100),
		VALID_FROM TIMESTAMP NOT NULL,
		VALID_TO TIMESTAMP NOT NULL,
		REQUEST_ID BIGINT
	) ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE DISTRIBUTE ON RANDOM';

	V_INS_YC := 'INSERT INTO SESSION.' || V_WRK_TBL || ' (
    	REPORTING_BASIS_ID,
	    CLOSING_DATE,
		ORIGIN_CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID, 
	    SEGMENT_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    ECONOMIC_DATA_AS_OF_DATE,
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
		USAGE_TYPE_ID,
		RATE,
		DISCOUNT_FACTOR_MYCF,
		SOURCE_IDENTIFIER,
		VALID_FROM,
		VALID_TO,
		REQUEST_ID
	)
	SELECT DISTINCT
	    REPORTING_BASIS_ID,
	    CLOSING_DATE, 
		NULL AS ORIGIN_CLOSING_DATE,
	    5 AS PARAMETER_TYPE_ID,
		NULL AS SEGMENT_TYPE_ID, 
	   	NULL AS SEGMENT_CODE,
	   	NULL AS SUBSIDIARY_CODE, 
	   	NULL AS SUBLEDGER_CODE, 
	   	NULL AS LOB_CODE, 
	   	NULL AS DOMAIN_CODE, 
	   	NULL AS ECONOMIC_DATA_AS_OF_DATE,
	   	NULL AS BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    1 AS MATURITY_TYPE_ID,  
	    Q.MATURITY,
		NULL AS USAGE_TYPE_ID,
		Q.RATE,
		NULL AS DISCOUNT_FACTOR_MYCF,
		NULL AS SOURCE_IDENTIFIER,
		VALID_FROM,
		VALID_TO,
		' || P_REQUEST_ID || ' AS REQUEST_ID
	FROM 		
		(
			SELECT DISTINCT
		        rb.id AS REPORTING_BASIS_ID,
		        a.clodat_d AS CLOSING_DATE,
		    	a.cur_cf AS CURRENCY_CODE,
				b.cre_d AS VALID_FROM,
				COALESCE(
					LEAD(b.cre_d, 1) OVER (
						PARTITION BY a.clodat_d, a.cur_cf
						ORDER BY a.clodat_d, a.cur_cf, b.cre_d, decode(a.per_cf, ''POC'', 1, ''POS'', 2, ''INV'', 3) DESC
					),
					''9999-12-31-00.00.00''
				) AS VALID_TO,
		        b.an1,b.an2,b.an3,b.an4,b.an5,b.an6,b.an7,b.an8,b.an9,b.an10,b.an11,b.an12,b.an13,b.an14,b.an15,b.an16,b.an17,b.an18,b.an19,b.an20,b.an21,b.an22,b.an23,b.an24,b.an25,b.an26,b.an27,b.an28,b.an29,b.an30,b.an31,b.an32,b.an33,b.an34,b.an35,b.an36,b.an37,b.an38,b.an39,b.an40,b.an41,b.an42,b.an43,b.an44,b.an45,b.an46,b.an47,b.an48,b.an49,b.an50,b.an51,b.an52,b.an53,b.an54,b.an55,b.an56,b.an57,b.an58,b.an59,b.an60,b.an61,b.an62,b.an63,b.an64,b.an65
		    FROM 
		    	BI_<env>.tpatsegsii a
		        INNER JOIN 
		        	BI_<env>.tpatternsii b
		         	ON a.pattern_id = b.pattern_id
		            AND a.cur_cf = b.cur_cf  
		        INNER JOIN
		        	BI_<env>.REPORTING_BASIS rb
		        	ON coalesce(a.norme_cf,b.norme_cf,''I17G'') = rb.code
		    WHERE 
		    	UPPER(a.patcat_ct) = ''DSC''
		        AND UPPER(a.pattyp_ct) = ''DSI''
		        AND UPPER(a.oripattyp_ct) = ''DSC''  
		        AND UPPER(b.pattyp_ct) = ''DSC'' 
		        AND a.clodat_d  >= ''2019-09-30-00.00.00''
		        AND a.per_cf in  (''POS'',''POC'',''INV'') 
		        AND coalesce(a.norme_cf,b.norme_cf,''I17G'') = ''I17G''
		) a,
		LATERAL (VALUES(substr(''an1'',3,1), an1),   (substr(''an2'',3,1), an2),   (substr(''an3'',3,1), an3),   (substr(''an4'',3,1), an4),   (substr(''an5'',3,1), an5),
	              (substr(''an6'',3,1), an6),   (substr(''an7'',3,1), an7),   (substr(''an8'',3,1), an8),   (substr(''an9'',3,1), an9),   (substr(''an10'',3,2), an10),
	              (substr(''an11'',3,2), an11), (substr(''an12'',3,2), an12), (substr(''an13'',3,2), an13), (substr(''an14'',3,2), an14), (substr(''an15'',3,2), an15),
	              (substr(''an16'',3,2), an16), (substr(''an17'',3,2), an17), (substr(''an18'',3,2), an18), (substr(''an19'',3,2), an19), (substr(''an20'',3,2), an20),
	              (substr(''an21'',3,2), an21), (substr(''an22'',3,2), an22), (substr(''an23'',3,2), an23), (substr(''an24'',3,2), an24), (substr(''an25'',3,2), an25),
	              (substr(''an26'',3,2), an26), (substr(''an27'',3,2), an27), (substr(''an28'',3,2), an28), (substr(''an29'',3,2), an29), (substr(''an30'',3,2), an30),
	              (substr(''an31'',3,2), an31), (substr(''an32'',3,2), an32), (substr(''an33'',3,2), an33), (substr(''an34'',3,2), an34), (substr(''an35'',3,2), an35),
	              (substr(''an36'',3,2), an36), (substr(''an37'',3,2), an37), (substr(''an38'',3,2), an38), (substr(''an39'',3,2), an39), (substr(''an40'',3,2), an40),
	              (substr(''an41'',3,2), an41), (substr(''an42'',3,2), an42), (substr(''an43'',3,2), an43), (substr(''an44'',3,2), an44), (substr(''an45'',3,2), an45),
	              (substr(''an46'',3,2), an46), (substr(''an47'',3,2), an47), (substr(''an48'',3,2), an48), (substr(''an49'',3,2), an49), (substr(''an50'',3,2), an50),
	              (substr(''an51'',3,2), an51), (substr(''an52'',3,2), an52), (substr(''an53'',3,2), an53), (substr(''an54'',3,2), an54), (substr(''an55'',3,2), an55),
	              (substr(''an56'',3,2), an56), (substr(''an57'',3,2), an57), (substr(''an58'',3,2), an58), (substr(''an59'',3,2), an59), (substr(''an60'',3,2), an60),
	              (substr(''an61'',3,2), an61), (substr(''an62'',3,2), an62), (substr(''an63'',3,2), an63), (substr(''an64'',3,2), an64), (substr(''an65'',3,2), an65)   
	              ) AS Q(MATURITY, RATE)
	WHERE  		
		VALID_FROM <> VALID_TO';
	             
	V_INS_IN := 'INSERT INTO SESSION.' || V_WRK_TBL || ' (
	    REPORTING_BASIS_ID,
	    CLOSING_DATE,
		ORIGIN_CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID, 
	    SEGMENT_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    ECONOMIC_DATA_AS_OF_DATE,
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
		USAGE_TYPE_ID,
		RATE,
		DISCOUNT_FACTOR_MYCF,
		SOURCE_IDENTIFIER,
		VALID_FROM,
		VALID_TO,
		REQUEST_ID
	)
	SELECT DISTINCT
	    REPORTING_BASIS_ID,
	    CLOSING_DATE,
		NULL AS ORIGIN_CLOSING_DATE,
	    6 AS PARAMATER_TYPE_ID,
	    NULL AS SEGMENT_TYPE_ID, 
	    NULL AS SEGMENT_CODE,
	    NULL AS SUBSIDIARY_CODE,
	    NULL AS SUBLEDGER_CODE,
	    NULL AS LOB_CODE,
	    NULL AS DOMAIN_CODE,
	    NULL AS ECONOMIC_DATA_AS_OF_DATE,
	    NULL AS BASE_CURRENCY_CODE,	
	    CURRENCY_CODE,
	    1 AS MATURITY_TYPE_ID,  
	    Q.MATURITY,
		NULL AS USAGE_TYPE_ID,
		Q.RATE,
		NULL AS DISCOUNT_FACTOR_MYCF,
		NULL AS SOURCE_IDENTIFIER,
		VALID_FROM,
		VALID_TO,
	    ' || P_REQUEST_ID || ' AS REQUEST_ID  
	FROM 
		(
			SELECT 
				rb.id AS REPORTING_BASIS_ID,
				a.clodat_d AS CLOSING_DATE,
				a.cur_cf AS CURRENCY_CODE,
				a.cre_d AS VALID_FROM,
				COALESCE(lead(a.cre_d,1) OVER (PARTITION BY a.clodat_d,a.cur_cf
       				ORDER BY a.clodat_d,a.cur_cf,a.cre_d)
       				, ''9999-12-31-00.00.00'') AS VALID_TO,
			    b.an1,b.an2,b.an3,b.an4,b.an5,b.an6,b.an7,b.an8,b.an9,b.an10,b.an11,b.an12,b.an13,b.an14,b.an15,b.an16,b.an17,b.an18,b.an19,b.an20,b.an21,b.an22,b.an23,b.an24,b.an25,b.an26,b.an27,b.an28,b.an29,b.an30,b.an31,b.an32,b.an33,b.an34,b.an35,b.an36,b.an37,b.an38,b.an39,b.an40,b.an41,b.an42,b.an43,b.an44,b.an45,b.an46,b.an47,b.an48,b.an49,b.an50,b.an51,b.an52,b.an53,b.an54,b.an55,b.an56,b.an57,b.an58,b.an59,b.an60,b.an61,b.an62,b.an63,b.an64,b.an65
			FROM 
				BI_<env>.TPATSEGSII a
				INNER JOIN 
					BI_<env>.TPATTERNSII b
			  		ON (a.pattern_id = b.pattern_id
			 		AND a.cur_cf = b.cur_cf)
			    INNER JOIN
			    	BI_<env>.REPORTING_BASIS rb
			    	ON COALESCE(a.norme_cf,b.norme_cf,''I17G'') = rb.code 		
			WHERE 
				UPPER(b.pattyp_ct)  = ''INF''
				AND a.clodat_d >= ''2019-09-30-00.00.00''
				AND COALESCE(a.norme_cf,b.norme_cf,''SII'') = ''SII''
				AND a.per_cf IN (''POS'',''POC'',''INV'')
		) a,
		LATERAL (VALUES(substr(''an1'',3,1), an1), (substr(''an2'',3,1), an2),   (substr(''an3'',3,1), an3),   (substr(''an4'',3,1), an4),   (substr(''an5'',3,1), an5),
	              (substr(''an6'',3,1), an6),   (substr(''an7'',3,1), an7),   (substr(''an8'',3,1), an8),   (substr(''an9'',3,1), an9),   (substr(''an10'',3,2), an10),
	              (substr(''an11'',3,2), an11), (substr(''an12'',3,2), an12), (substr(''an13'',3,2), an13), (substr(''an14'',3,2), an14), (substr(''an15'',3,2), an15),
	              (substr(''an16'',3,2), an16), (substr(''an17'',3,2), an17), (substr(''an18'',3,2), an18), (substr(''an19'',3,2), an19), (substr(''an20'',3,2), an20),
	              (substr(''an21'',3,2), an21), (substr(''an22'',3,2), an22), (substr(''an23'',3,2), an23), (substr(''an24'',3,2), an24), (substr(''an25'',3,2), an25),
	              (substr(''an26'',3,2), an26), (substr(''an27'',3,2), an27), (substr(''an28'',3,2), an28), (substr(''an29'',3,2), an29), (substr(''an30'',3,2), an30),
	              (substr(''an31'',3,2), an31), (substr(''an32'',3,2), an32), (substr(''an33'',3,2), an33), (substr(''an34'',3,2), an34), (substr(''an35'',3,2), an35),
	              (substr(''an36'',3,2), an36), (substr(''an37'',3,2), an37), (substr(''an38'',3,2), an38), (substr(''an39'',3,2), an39), (substr(''an40'',3,2), an40),
	              (substr(''an41'',3,2), an41), (substr(''an42'',3,2), an42), (substr(''an43'',3,2), an43), (substr(''an44'',3,2), an44), (substr(''an45'',3,2), an45),
	              (substr(''an46'',3,2), an46), (substr(''an47'',3,2), an47), (substr(''an48'',3,2), an48), (substr(''an49'',3,2), an49), (substr(''an50'',3,2), an50),
	              (substr(''an51'',3,2), an51), (substr(''an52'',3,2), an52), (substr(''an53'',3,2), an53), (substr(''an54'',3,2), an54), (substr(''an55'',3,2), an55),
	              (substr(''an56'',3,2), an56), (substr(''an57'',3,2), an57), (substr(''an58'',3,2), an58), (substr(''an59'',3,2), an59), (substr(''an60'',3,2), an60),
	              (substr(''an61'',3,2), an61), (substr(''an62'',3,2), an62), (substr(''an63'',3,2), an63), (substr(''an64'',3,2), an64), (substr(''an65'',3,2), an65)      
	              ) AS Q(MATURITY, RATE)
	WHERE
		VALID_FROM <> VALID_TO';
	             
	V_INS_DR := 'INSERT INTO SESSION.' || V_WRK_TBL_2 || ' (
	    REPORTING_BASIS_ID,
	    CLOSING_DATE,
		ORIGIN_CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID, 
	    SEGMENT_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    ECONOMIC_DATA_AS_OF_DATE,
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
		USAGE_TYPE_ID,
		RATE,
		DISCOUNT_FACTOR,
		DISCOUNT_FACTOR_MYCF,
		SOURCE_IDENTIFIER,
		VALID_FROM,
		VALID_TO,
		REQUEST_ID
	)
	SELECT
		REPORTING_BASIS_ID,
	    CLOSING_DATE,
		ORIGIN_CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID, 
	    SEGMENT_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    ECONOMIC_DATA_AS_OF_DATE,
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
		USAGE_TYPE_ID,
		RATE,
		CASE 
			WHEN POWER((1 + RATE),(DECFLOAT(MATURITY) / 12)) = 0 THEN 0 
			ELSE  (1 /( POWER((1 + RATE),(DECFLOAT(MATURITY) / 12)) )) 
		END AS DISCOUNT_FACTOR, 
		DISCOUNT_FACTOR_MYCF,
		SOURCE_IDENTIFIER,
		VALID_FROM,
		VALID_TO,
		REQUEST_ID
	FROM
		SESSION.' || V_WRK_TBL || '
	GROUP BY
		REPORTING_BASIS_ID,
	    CLOSING_DATE,
	    ORIGIN_CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID, 
	    SEGMENT_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    ECONOMIC_DATA_AS_OF_DATE,
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
	    USAGE_TYPE_ID,
		RATE,
		DISCOUNT_FACTOR_MYCF,
		SOURCE_IDENTIFIER,
		VALID_FROM,
		VALID_TO,
		REQUEST_ID';
	
	V_INS_DEF := 'INSERT INTO SESSION.' || V_WRK_TBL_2 || ' (
	    REPORTING_BASIS_ID,
	    CLOSING_DATE,
		ORIGIN_CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID, 
	    SEGMENT_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    ECONOMIC_DATA_AS_OF_DATE,
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
		USAGE_TYPE_ID,
		RATE,
		DISCOUNT_FACTOR,
		DISCOUNT_FACTOR_MYCF,
		SOURCE_IDENTIFIER,
		VALID_FROM,
		VALID_TO,
		REQUEST_ID
	)
	SELECT DISTINCT
		rb.ID AS REPORTING_BASIS_ID,
		a.clodat_d AS CLOSING_DATE,
		NULL AS ORIGIN_CLOSING_DATE,
	    6 AS PARAMATER_TYPE_ID,
	    NULL AS SEGMENT_TYPE_ID, 
	    ''Default'' AS SEGMENT_CODE,
	    NULL AS SUBSIDIARY_CODE,
	    NULL AS SUBLEDGER_CODE,
	    NULL AS LOB_CODE,
	    NULL AS DOMAIN_CODE,
	    NULL AS ECONOMIC_DATA_AS_OF_DATE,
	    NULL AS BASE_CURRENCY_CODE,
		''999'' AS CURRENCY_CODE,
		1 AS MATURITY_TYPE_ID,  
	    Q.MATURITY,
		NULL AS USAGE_TYPE_ID,
		0 AS RATE,	
		0 AS DISCOUNT_FACTOR,
		NULL AS DISCOUNT_FACTOR_MYCF,
		NULL AS SOURCE_IDENTIFIER,
	    a.CRE_D AS VALID_FROM,
	    TO_TIMESTAMP(''9999-12-31'',''YYYY-MM-DD'') as VALID_TO,
		' || P_REQUEST_ID || ' AS REQUEST_ID
	FROM 
		BI_<env>.TPATSEGSII a,
	 	BI_<env>.TPATTERNSII b,  
		TABLE (
			VALUES
				(substr(''an1'',3,1), b.an1),   (substr(''an2'',3,1), b.an2),   (substr(''an3'',3,1), b.an3),   (substr(''an4'',3,1), b.an4),   (substr(''an5'',3,1), b.an5),
	        	(substr(''an6'',3,1), b.an6),   (substr(''an7'',3,1), b.an7),   (substr(''an8'',3,1), b.an8),   (substr(''an9'',3,1), b.an9),   (substr(''an10'',3,2), b.an10),
	        	(substr(''an11'',3,2), b.an11), (substr(''an12'',3,2), b.an12), (substr(''an13'',3,2), b.an13), (substr(''an14'',3,2), b.an14), (substr(''an15'',3,2), b.an15),
	            (substr(''an16'',3,2), b.an16), (substr(''an17'',3,2), b.an17), (substr(''an18'',3,2), b.an18), (substr(''an19'',3,2), b.an19), (substr(''an20'',3,2), b.an20),
	            (substr(''an21'',3,2), b.an21), (substr(''an22'',3,2), b.an22), (substr(''an23'',3,2), b.an23), (substr(''an24'',3,2), b.an24), (substr(''an25'',3,2), b.an25),
	            (substr(''an26'',3,2), b.an26), (substr(''an27'',3,2), b.an27), (substr(''an28'',3,2), b.an28), (substr(''an29'',3,2), b.an29), (substr(''an30'',3,2), b.an30),
	            (substr(''an31'',3,2), b.an31), (substr(''an32'',3,2), b.an32), (substr(''an33'',3,2), b.an33), (substr(''an34'',3,2), b.an34), (substr(''an35'',3,2), b.an35),
	            (substr(''an36'',3,2), b.an36), (substr(''an37'',3,2), b.an37), (substr(''an38'',3,2), b.an38), (substr(''an39'',3,2), b.an39), (substr(''an40'',3,2), b.an40),
	            (substr(''an41'',3,2), b.an41), (substr(''an42'',3,2), b.an42), (substr(''an43'',3,2), b.an43), (substr(''an44'',3,2), b.an44), (substr(''an45'',3,2), b.an45),
	            (substr(''an46'',3,2), b.an46), (substr(''an47'',3,2), b.an47), (substr(''an48'',3,2), b.an48), (substr(''an49'',3,2), b.an49), (substr(''an50'',3,2), b.an50),
	            (substr(''an51'',3,2), b.an51), (substr(''an52'',3,2), b.an52), (substr(''an53'',3,2), b.an53), (substr(''an54'',3,2), b.an54), (substr(''an55'',3,2), b.an55),
	            (substr(''an56'',3,2), b.an56), (substr(''an57'',3,2), b.an57), (substr(''an58'',3,2), b.an58), (substr(''an59'',3,2), b.an59), (substr(''an60'',3,2), b.an60),
	            (substr(''an61'',3,2), b.an61), (substr(''an62'',3,2), b.an62), (substr(''an63'',3,2), b.an63), (substr(''an64'',3,2), b.an64), (substr(''an65'',3,2), b.an65)      
	    ) AS Q(MATURITY, RATE)
	    INNER JOIN
		BI_<env>.REPORTING_BASIS rb
			ON COALESCE(a.norme_cf, b.norme_cf,''I17G'') = rb.code 
	WHERE 
		a.pattern_id = b.pattern_id
		AND a.cur_cf = b.cur_cf
		AND UPPER(b.pattyp_ct) = ''INF''
		AND a.clodat_d IN (
			SELECT 
				MAX(a.clodat_d)
			FROM 
				BI_<env>.tpatsegsii a
				JOIN 
					BI_<env>.tpatternsii b
					ON a.pattern_id = b.pattern_id
					AND a.cur_cf = b.cur_cf
					WHERE UPPER(b.pattyp_ct)  = ''INF''
					AND a.clodat_d >= ''2019-09-30''
					AND coalesce(a.norme_cf,b.norme_cf,''SII'' ) = ''SII''
					AND a.per_cf = ''POC''
		)
		AND a.per_cf =''POC'' 
		AND coalesce(a.norme_cf,b.norme_cf,''SII'') = ''SII''';
	
	V_INS_DWH := 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
	    REPORTING_BASIS_ID,
	    CLOSING_DATE,
		ORIGIN_CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID, 
	    SEGMENT_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    ECONOMIC_DATA_AS_OF_DATE,
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
		USAGE_TYPE_ID,
		RATE,
		DISCOUNT_FACTOR, 
		DISCOUNT_FACTOR_MYCF,
		SOURCE_IDENTIFIER,
		HASH_KEY,  
		VALID_FROM,
		VALID_TO,
		PIVOT_KEY,
		SUPP_DATE,
		CREATED_REQUEST_ID,
		MODIFIED_REQUEST_ID,
		DELETED_REQUEST_ID
	)
	SELECT
		REPORTING_BASIS_ID,
	    CLOSING_DATE,
		ORIGIN_CLOSING_DATE,
	    PARAMETER_TYPE_ID,
	    SEGMENT_TYPE_ID, 
	    SEGMENT_CODE,
	    SUBSIDIARY_CODE,
	    SUBLEDGER_CODE,
	    LOB_CODE,
	    DOMAIN_CODE,
	    ECONOMIC_DATA_AS_OF_DATE,
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
		(SELECT ID FROM BI_<env>.USAGE_TYPE WHERE CODE = ''PROD'') AS USAGE_TYPE_ID,
		RATE,
		DISCOUNT_FACTOR, 
		DISCOUNT_FACTOR_MYCF,
		SOURCE_IDENTIFIER,
		' || V_HASH_KEY || ' AS HASH_KEY,  
		VALID_FROM,
		VALID_TO,
		' || V_PIVOT_KEY || ' AS PIVOT_KEY,
		''' || V_SUPP_TS || ''' AS SUPP_DATE,
		REQUEST_ID AS CREATED_REQUEST_ID,
		NULL AS MODIFIED_REQUEST_ID,
		NULL AS DELETED_REQUEST_ID
	FROM
		SESSION.' || V_WRK_TBL_2 || '';
	
	EXECUTE IMMEDIATE 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE CREATED_REQUEST_ID = ' || P_REQUEST_ID;

	RAISE NOTICE 'Executing V_INS_YC: ''%''', V_INS_YC;

	EXECUTE IMMEDIATE V_INS_YC;

	RAISE NOTICE 'Executing V_INS_IN: ''%''', V_INS_IN;

	EXECUTE IMMEDIATE V_INS_IN;

	RAISE NOTICE 'Executing V_INS_DR: ''%''', V_INS_DR;

	EXECUTE IMMEDIATE V_INS_DR;

	RAISE NOTICE 'Executing V_INS_DEF: ''%''', V_INS_DEF;

	EXECUTE IMMEDIATE V_INS_DEF;

	RAISE NOTICE 'Executing V_INS_DWH: ''%''', V_INS_DWH;

	EXECUTE IMMEDIATE V_INS_DWH;

	EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TBL || ' IF EXISTS';

	EXECUTE IMMEDIATE 'DROP TABLE SESSION.' || V_WRK_TBL_2 || ' IF EXISTS';

	DROP TABLE SESSION.KEYS IF EXISTS;

EXCEPTION 
	WHEN OTHERS THEN 
		L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
		L_ERR_MSG := SQLERRM; 
		RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
		RETURN L_ERR_CD;

END;

END_PROC;