SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_LIFE_TREATY_TYPE AS
SELECT
    CAST("Life_Treaty_Type" AS CHAR(2)) AS "Life_Treaty_Type",
    CAST("Life_Treaty_Type_Description" AS VARCHAR(16)) AS "Life_Treaty_Type_Description"
FROM
    (
    SELECT
        TBA.COLVAL_CT AS "Life_Treaty_Type",
        TBA.COLVAL_LS AS "Life_Treaty_Type_Description"
    FROM
        BI_<env>.TBANALL TBA
    WHERE
        TBA.COL_LS = 'LIFTRTTYP_CF'
        AND TBA.LAG_CF = 'E'
    GROUP BY
        TBA.COLVAL_CT,
        TBA.COLVAL_LS);