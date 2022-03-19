SET SCHEMA STAGING_<env>;

DROP PROCEDURE SP_LOAD_WRK_CAN_AXIS_PROJECTION_VALIDATION;

CREATE OR REPLACE PROCEDURE SP_LOAD_WRK_CAN_AXIS_PROJECTION_VALIDATION (BIGINT,CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64),CHARACTER VARYING(64)) RETURNS INTEGER 
LANGUAGE nzplsql AS 
BEGIN_PROC 

DECLARE 
    P_REQUEST_ID	        ALIAS FOR $1;
    P_SRC_SCHEMA			ALIAS FOR $2;
    P_SRC_TABLE				ALIAS FOR $3;
    P_TRG_SCHEMA			ALIAS FOR $4;
    P_TRG_TABLE				ALIAS FOR $5;
    L_ERR_CD				CHAR(5);
    L_ERR_MSG				VARCHAR(32000);
    V_REC					RECORD;
    V_DELETE_QUERY			VARCHAR(32000);
    V_INSERT_QUERY			VARCHAR(32000);
	V_TEMP_QUERY			VARCHAR(32000);

BEGIN
SET ISOLATION TO UR;

V_DELETE_QUERY := 'DELETE FROM ' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || ' WHERE REQUEST_ID = ' || P_REQUEST_ID || '';

V_INSERT_QUERY := 'INSERT INTO 	' || P_TRG_SCHEMA || '.' || P_TRG_TABLE || '
(   CONTRACT_NUMBER,
	SECTION_NUMBER,
	ASSUMED_CONTRACT_NUMBER,
	ASSUMED_SECTION_NUMBER,
	UNDERWRITING_ORDER,
	ENDORSEMENT_NUMBER,
	SPLIT_TYPE_ID,
	AOC_STEP,
	LEVEL_OF_ANALYSIS_ID,
	SENSITIVITY_TYPE,
	SCENARIO_TYPE_ID,
	SENSITIVITY_VALUE,
	SCENARIO_PARAMETER,
	POLICY_UW_YEAR,
	UNDERWRITING_YEAR,
	BUSINESS_MATURITY,
	NEW_BUSINESS_IND,
	BUSINESS_MATURITY_ID,
	POSITION,
	POSITION_ID,
	CURRENCY_CODE,
	BASIS,
	REPORTING_BASIS_ID,
	CLOSING_DATE,
	PERM_TERM_TYPE,
	HISTORIZATION_MODE,
	P_TIME,
	PROJECTION_MONTH,
	PROJECTION_YEAR,
	PROJECTION_DATE,
	PERIOD_TYPE_ID,
	PERIOD_DESCRIPTION,
	AMOUNT,
	CREATED_BY,
	CREATED_DATE,
	LINE_NUMBER,
	REQUEST_ID,
	SOURCE_REF_NAME)
SELECT
	SRC.CONTRACT_NUMBER,
	SRC.SECTION_NUMBER,
	SRC.ASSUMED_CONTRACT_NUMBER,
	SRC.ASSUMED_SECTION_NUMBER,
	SRC.UNDERWRITING_ORDER,
	SRC.ENDORSEMENT_NUMBER,
	SRC.SPLIT_TYPE_ID,
	SRC.AOC_STEP,
	SRC.LEVEL_OF_ANALYSIS_ID,
	SRC.SENSITIVITY_TYPE,
	SRC.SCENARIO_TYPE_ID,
	SRC.SENSITIVITY_VALUE,
	SRC.SCENARIO_PARAMETER,
	SRC.POLICY_UW_YEAR,
	SRC.UNDERWRITING_YEAR,
	SRC.BUSINESS_MATURITY,
	SRC.NEW_BUSINESS_IND,
	SRC.BUSINESS_MATURITY_ID,
	SRC.POSITION,
	SRC.POSITION_ID,
	SRC.CURRENCY_CODE,
	SRC.BASIS,
	SRC.REPORTING_BASIS_ID,
	SRC.CLOSING_DATE,
	SRC.PERM_TERM_TYPE,
	SRC.HISTORIZATION_MODE,
	SRC.P_TIME,
	SRC.PROJECTION_MONTH,
	SRC.PROJECTION_YEAR,
	SRC.PROJECTION_DATE,
	SRC.PERIOD_TYPE_ID,
	SRC.PERIOD_DESCRIPTION,
	CASE WHEN SRC.UNDERWRITING_ORDER = 1 AND SRC.ENDORSEMENT_NUMBER = 0 THEN CAST(SRC.AMOUNT * SIGN_CONVENTION AS DECFLOAT)
		ELSE CAST(SRC.AMOUNT * SIGN_CONVENTION * RETRO_FACTOR AS DECFLOAT)
	END AS AMOUNT,
	SRC.CREATED_BY,
	SRC.CREATED_DATE,
	SRC.LINE_NUMBER,
	SRC.REQUEST_ID,
	SRC.SOURCE_REF_NAME
FROM (SELECT
		SRC.CONTRACT_NUMBER,
		SRC.SECTION_NUMBER,
		SRC.ASSUMED_CONTRACT_NUMBER,
		SRC.ASSUMED_SECTION_NUMBER,
		SRC.UNDERWRITING_ORDER,
		SRC.ENDORSEMENT_NUMBER,
		SRC.SPLIT_TYPE_ID,
		SRC.AOC_STEP,
		SRC.LEVEL_OF_ANALYSIS_ID,
		SRC.SENSITIVITY_TYPE,
		SRC.SCENARIO_TYPE_ID,
		SRC.SENSITIVITY_VALUE,
		SRC.SCENARIO_PARAMETER,
		SRC.POLICY_UW_YEAR,
		SRC.UNDERWRITING_YEAR,
		SRC.BUSINESS_MATURITY,
		SRC.NEW_BUSINESS_IND,
		SRC.BUSINESS_MATURITY_ID,
		SRC.POSITION,
		SRC.POSITION_ID,
		SRC.CURRENCY_CODE,
		SRC.BASIS,
		SRC.REPORTING_BASIS_ID,
		SRC.CLOSING_DATE,
		SRC.PERM_TERM_TYPE,
		SRC.HISTORIZATION_MODE,
		SRC.P_TIME,
		SRC.PROJECTION_MONTH,
		SRC.PROJECTION_YEAR,
		SRC.PROJECTION_DATE,
		SRC.PERIOD_TYPE_ID,
		SRC.PERIOD_DESCRIPTION,
		SRC.AMOUNT,
		SRC.CREATED_BY,
		SRC.CREATED_DATE,
		SRC.LINE_NUMBER,
		SRC.REQUEST_ID,
		SRC.SOURCE_REF_NAME
	FROM ' || P_SRC_SCHEMA || '.' || P_SRC_TABLE || ' SRC
	WHERE REQUEST_ID = ' || P_REQUEST_ID || '
		AND NOT EXISTS (
		SELECT
			1
		FROM 			(
			SELECT
				ERR.ERROR_ROW
			FROM DELIVERY_<env>.UPLOAD_ERROR_LOG ERR
			WHERE ERR.REQUEST_ID = ' || P_REQUEST_ID || '
				AND ERR.ERROR_MESSAGE_ID IN (
				SELECT
					ERROR_MESSAGE_ID
				FROM DELIVERY_<env>.ERROR_MESSAGE
				WHERE MESSAGE_TYPE_FLAG = ''F''
					AND REJECTION_TYPE = ''Reject'' )
			GROUP BY
				ERR.ERROR_ROW)
		WHERE ERROR_ROW = SRC.LINE_NUMBER)
	)SRC
LEFT JOIN (
	SELECT 
		UCASE(POS.NAME) as NAME ,
		POS.SIGN_CONVENTION,
		POS.RETRO_FACTOR
	FROM
		BI_<env>.POSITION POS
	INNER JOIN BI_<env>.POSITION_TYPE PT ON PT.ID=POS.POSITION_TYPE_ID AND PT.CODE = ''RDWH''
) POS ON UCASE(SRC.POSITION) = POS.NAME
'; 

RAISE NOTICE 'Executing V_DELETE_QUERY: ''%''',V_DELETE_QUERY; 
EXECUTE IMMEDIATE V_DELETE_QUERY; 

RAISE NOTICE 'Executing V_INSERT_QUERY: ''%''',V_INSERT_QUERY;
EXECUTE IMMEDIATE V_INSERT_QUERY;

EXCEPTION WHEN OTHERS THEN L_ERR_CD := SUBSTR(SQLERRM, 8, 5); 
	L_ERR_MSG := SQLERRM; 
	RAISE EXCEPTION '% Error while executing SQL statement', L_ERR_MSG; 
	RETURN L_ERR_CD; 
 
END; 

END_PROC;