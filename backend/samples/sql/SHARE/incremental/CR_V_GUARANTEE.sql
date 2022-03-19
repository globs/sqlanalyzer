SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_GUARANTEE AS
SELECT
    CAST("Guarantee" AS CHAR(3)) AS "Guarantee",
    CAST("Guarantee_Description" AS VARCHAR(16)) AS "Guarantee_Description"
FROM
    (
    SELECT
        TGA.GAR_CF AS "Guarantee",
        TGA.GAR_GS AS "Guarantee_Description"
    FROM
        BI_<env>.TGARL TGA
    WHERE
        TGA.LAG_CF = 'E'
    GROUP BY
        TGA.GAR_CF,
        TGA.GAR_GS);