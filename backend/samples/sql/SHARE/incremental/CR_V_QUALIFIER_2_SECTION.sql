SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_QUALIFIER_2_SECTION AS
SELECT
    CAST("Qualifier_2_Section" AS CHAR(5)) AS "Qualifier_2_Section",
    CAST("Qualifier_2_Section_Description" AS VARCHAR(16)) AS "Qualifier_2_Section_Description"
FROM
    (
    SELECT
        TBA.COLVAL_CT AS "Qualifier_2_Section",
        TBA.COLVAL_LS AS "Qualifier_2_Section_Description"
    FROM
        BI_<env>.TBANALL TBA
    WHERE
        TBA.COL_LS = 'CTRQUA2_CF'
        AND TBA.LAG_CF = 'E'
    GROUP BY
        TBA.COLVAL_CT,
        TBA.COLVAL_LS);