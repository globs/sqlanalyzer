SET SCHEMA STAGING_<env>;

CREATE OR REPLACE VIEW V_COL_ERROR_CHECK_NORMA_FORPLAN_STAGING AS
(
    SELECT
        SRA.LINE_NUMBER
        , 2              AS COL_NUMBER
        , SRA.LOB_N1     AS COL_VALUE
        , 74             AS ERROR_MESSAGE_ID
    FROM STAGING_RISK_ADJUSTMENT_FORPLAN SRA
    LEFT OUTER JOIN BI_<env>.LOB_N1  TLOB ON (SRA.LOB_N1 = TLOB.LOB_N1 AND TLOB.LANGUAGE='E')
    WHERE (TLOB.LOB_N1 IS NULL )
UNION
    SELECT
        SRA.LINE_NUMBER
        , 3              AS COL_NUMBER
        , SRA.NATURE     AS COL_VALUE
        , 63             AS ERROR_MESSAGE_ID
    FROM STAGING_RISK_ADJUSTMENT_FORPLAN SRA
    WHERE SRA.NATURE NOT IN ('F','N','P')
)
;
