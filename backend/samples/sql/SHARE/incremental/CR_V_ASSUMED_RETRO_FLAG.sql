SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_ASSUMED_RETRO_FLAG AS
SELECT
    "Assumed/Retro_Flag",
    CAST("Assumed/Retro_Flag_Description" AS VARCHAR(16)) AS "Assumed/Retro_Flag_Description"
FROM
    (
    SELECT
        'A' AS "Assumed/Retro_Flag",
        'Assumed' AS "Assumed/Retro_Flag_Description"
    FROM
        SYSIBM.SYSDUMMY1
UNION
    SELECT
        'R' AS "Assumed/Retro_Flag",
        'Retro' AS "Assumed/Retro_Flag_Description"
    FROM
        SYSIBM.SYSDUMMY1);