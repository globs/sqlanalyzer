SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_YCMS_ECONOMIC_RATE;

CREATE OR REPLACE PROCEDURE SP_LOAD_YCMS_ECONOMIC_RATE(
	BIGINT,
	CHARACTER VARYING(64),
	CHARACTER VARYING(64),
	CHARACTER VARYING(64),
	CHARACTER VARYING(64)
) 
RETURNS INTEGER
language nzplsql
AS
BEGIN_PROC

DECLARE
	P_REQUEST_ID   ALIAS FOR $1;
    P_SRC_SCHEMA   ALIAS FOR $2;
    P_SRC_TABLE	   ALIAS FOR $3;
    P_TRG_SCHEMA   ALIAS FOR $4;
    P_TRG_TABLE	   ALIAS FOR $5;
	V_INS_SQL      VARCHAR(ANY);
	L_ERR_CD       CHAR(5);
	L_ERR_MSG      VARCHAR(32000);

BEGIN	
	SET ISOLATION TO UR;
	
	V_INS_SQL := 'INSERT INTO ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' (
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
	    BASE_CURRENCY_CODE,
	    CURRENCY_CODE,
	    MATURITY_TYPE_ID, 
	    MATURITY,
		USAGE_TYPE_ID,
		RATE,
		DISCOUNT_FACTOR,
		DISCOUNT_FACTOR_MYCF,
		REQUEST_ID
	)
	SELECT DISTINCT
		RB.ID AS REPORTING_BASIS_ID,
		CASE
			WHEN 
				RIGHT(DATE_QUARTER, 2) = ''Q1'' 
			THEN 
				TO_DATE(LEFT(DATE_QUARTER, 4) || ''0331'', ''yyyymmdd'')
			WHEN 
				RIGHT(DATE_QUARTER, 2) = ''Q2'' 
			THEN 
				TO_DATE(LEFT(DATE_QUARTER, 4) || ''0630'', ''yyyymmdd'')
			WHEN 
				RIGHT(DATE_QUARTER, 2) = ''Q3'' 
			THEN 
				TO_DATE(LEFT(DATE_QUARTER, 4) || ''0930'', ''yyyymmdd'')
			WHEN 
				RIGHT(DATE_QUARTER, 2) = ''Q4'' 
			THEN 
				TO_DATE(LEFT(DATE_QUARTER, 4) || ''1231'', ''yyyymmdd'')
			ELSE 
				NULL
		END AS CLOSING_DATE,
		CASE
			WHEN 
				RIGHT(ORIGIN_QUARTER, 2) = ''Q1'' 
			THEN 
				TO_DATE(LEFT(ORIGIN_QUARTER, 4) || ''0331'', ''yyyymmdd'')
			WHEN 
				RIGHT(ORIGIN_QUARTER, 2) = ''Q2'' 
			THEN 
				TO_DATE(LEFT(ORIGIN_QUARTER, 4) || ''0630'', ''yyyymmdd'')
			WHEN 
				RIGHT(ORIGIN_QUARTER, 2) = ''Q3'' 
			THEN 
				TO_DATE(LEFT(ORIGIN_QUARTER, 4) || ''0930'', ''yyyymmdd'')
			WHEN 
				RIGHT(ORIGIN_QUARTER, 2) = ''Q4'' 
			THEN 
				TO_DATE(LEFT(ORIGIN_QUARTER, 4) || ''1231'', ''yyyymmdd'')
			ELSE 
				NULL
		END AS ORIGIN_CLOSING_DATE,
		PT.ID AS PARAMETER_TYPE_ID,
		SC.ID AS SEGMENT_TYPE_ID,
		REGEXP_EXTRACT(CURVE_BUCKET, ''(\d+$)'') AS SEGMENT_CODE,
		CAST(SUBSIDIARY AS SMALLINT) AS SUBSIDIARY_CODE,
		CAST(SUBLEDGER AS SMALLINT) AS SUBLEDGER_CODE,
		CASE
			WHEN 
				UPPER(CURVE_DIVISION) = ''LIFE'' 
			THEN 
				''30''
			WHEN 
				UPPER(CURVE_DIVISION) = ''P&C'' 
			THEN 
				NULL
			ELSE 
				NULL
		END AS LOB_CODE,
		NULL AS DOMAIN_CODE,
		NULL AS BASE_CURRENCY_CODE,
		CURVE_CURRENCY AS CURRENCY_CODE,
		MT.ID AS MATURITY_TYPE_ID,
		CASE
			WHEN 
				UPPER(CURVE_MATURITY) = ''M'' 
			THEN 
				MATURITY_MONTHS
			WHEN 
				UPPER(CURVE_MATURITY) IN (''Y'',''A'') 
			THEN 
				MATURITY_YEARS
		ELSE NULL
		END MATURITY,
		UT.ID AS USAGE_TYPE_ID,
		CAST(RATE AS DECFLOAT) AS RATE,
		CASE
			WHEN 
				UPPER(CURVE_DIVISION) = ''LIFE'' 
				AND DISCOUNT_FACTOR IS NULL 
				AND UPPER(CURVE_MATURITY) = ''M'' 
			THEN 
				CAST(1/power(1+RATE,MATURITY_MONTHS/12) AS DECFLOAT)
			WHEN 
				UPPER(CURVE_DIVISION) = ''LIFE'' 
				AND DISCOUNT_FACTOR IS NULL 
				AND UPPER(CURVE_MATURITY) IN (''Y'',''A'') 
			THEN 
				CAST(1/power(1+RATE,MATURITY_YEARS) AS DECFLOAT)
			ELSE 
				CAST(DISCOUNT_FACTOR AS DECFLOAT)
		END AS DISCOUNT_FACTOR,
		CASE
			WHEN 
				CURVE_TYPE = ''YC'' 
				AND CURVE_DESCRIPTION = ''FWD'' 
				AND UPPER(CURVE_DIVISION) = ''P&C'' 
			THEN 
				DISC_FACT_MYCF
			ELSE 
				NULL
		END AS DISCOUNT_FACTOR_MYCF,
		' || P_REQUEST_ID || ' AS REQUEST_ID
	FROM 
		' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' YCMS
		INNER JOIN
			(
				SELECT 
					MAX(CURVE_ID_YC) AS YC_ID
				FROM 
					STAGING_UAT.YCMS_OUTPUTS_FWD
				GROUP BY 
					GAAP,
					DATE_QUARTER,
					ORIGIN_QUARTER,
					CURVE_BUCKET,
					SUBSIDIARY,
					SUBLEDGER,
					CURVE_DIVISION,
					CURVE_CURRENCY,
					CURVE_MATURITY,
					MATURITY_YEARS,
					MATURITY_MONTHS,
					USAGE_TARGET
		    ) l_id
			ON l_id.YC_ID = YCMS.CURVE_ID_YC
		INNER JOIN 
			BI_<env>.REPORTING_BASIS RB
			ON YCMS.GAAP = RB.CODE
		INNER JOIN 
			BI_<env>.PARAMETER_TYPE PT
			ON YCMS.CURVE_TYPE = PT.GROUP_CODE
			AND COALESCE(
				CASE
					WHEN 
						CURVE_TYPE = ''YC'' 
						AND CURVE_DESCRIPTION = ''LKI'' 
						AND DATE_QUARTER = ORIGIN_QUARTER 
					THEN 
						''ZCR''
					WHEN 
						CURVE_TYPE = ''YC'' 
						AND CURVE_DESCRIPTION = ''FWD'' 
					THEN 
						''LKI''
					ELSE 
						CURVE_DESCRIPTION
				END,
				YCMS.CURVE_TYPE
			) = PT.CODE
		INNER JOIN 
			BI_<env>.MATURITY_TYPE MT
			ON decode(YCMS.CURVE_MATURITY,''A'',''Y'',''M'',''M'',''T'',''Q'') = LEFT(MT.CODE, 1)
		LEFT OUTER JOIN 
			BI_<env>.SEGMENT_TYPE SC
			ON REGEXP_EXTRACT(YCMS.CURVE_BUCKET, ''(^[a-zA-Z]+)'') = SC.NAME
		LEFT JOIN BI_<env>.USAGE_TYPE UT
			ON UPPER(YCMS.USAGE_TARGET) = UPPER(UT.CODE)';
			
		EXECUTE IMMEDIATE 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE REQUEST_ID = ' || P_REQUEST_ID;
	
		RAISE NOTICE 'Executing V_INS_SQL: ''%''', V_INS_SQL;
	
		EXECUTE IMMEDIATE V_INS_SQL;
		
EXCEPTION 
	WHEN OTHERS THEN
   		L_ERR_CD := SUBSTR(SQLERRM, 8, 5);
		L_ERR_MSG := SQLERRM;
		RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG;
		RETURN L_ERR_CD;
		
END

END_PROC;