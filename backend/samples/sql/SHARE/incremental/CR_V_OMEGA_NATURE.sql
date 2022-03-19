SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_OMEGA_NATURE AS
SELECT
    CAST("Nature" AS CHAR(2)) AS "Nature",
    CAST("Nature_Description" AS VARCHAR(16)) AS "Nature_Description"
FROM
    (
    SELECT
        TCT.CTRNAT_CF AS "Nature",
        TCT.CTRNAT_GS AS "Nature_Description"
    FROM
        BI_<env>.TCTRNATL TCT
    WHERE
        TCT.LAG_CF = 'E'
    GROUP BY
        TCT.CTRNAT_CF,
        TCT.CTRNAT_GS);