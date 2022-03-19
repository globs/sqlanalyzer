SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_UNDERWRITING_UNIT AS
SELECT
    CAST("Underwriting_Unit" AS SMALLINT) AS "Underwriting_Unit",
    CAST("Underwriting_Unit_Description" AS VARCHAR(16)) AS "Underwriting_Unit_Description"
FROM
    (
    SELECT
        TCO.UWGRP_CF AS "Underwriting_Unit",
        TCO.UWGRP_LS AS "Underwriting_Unit_Description"
    FROM
        BI_<env>.TCONTR TCO
    WHERE
        TCO.CTRTYP_CT = 1
        AND TCO.SUPP_D = '9999-12-31'
        AND TCO.UWGRP_LS IS NOT NULL
    GROUP BY
        TCO.UWGRP_CF,
        TCO.UWGRP_LS);