SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_MACRO_MARKET AS
SELECT
    "Macro_Market",
    CAST("Macro_Market_Description" AS VARCHAR(16)) AS "Macro_Market_Description"
FROM
    (
    SELECT
        MRKUNT_NT AS "Macro_Market",
        MRKUNT_LS AS "Macro_Market_Description"
    FROM
        BI_<env>.TUWSEC
    WHERE
        SUBLEDGER_CF NOT IN ('05-01',
        '05-10',
        '06-01',
        '07-01',
        '10-01',
        '16-01',
        '18-16',
        '18-17',
        '20-01',
        '20-02',
        '26-01',
        '26-02',
        '26-03',
        '26-05',
        '26-06',
        '26-07',
        '26-08',
        '26-09',
        '26-10')
        AND SECSTS_CT <> 25
        AND SSD_CF NOT IN (14,
        25)
        AND ACTIV2_CF = '400  '
        AND MRKUNT_NT IS NOT NULL
        AND BUSUNIT_CF IS NOT NULL
        AND END_D = '9999-12-31'
        AND SUPP_D = '9999-12-31'
    GROUP BY
        MRKUNT_NT,
        MRKUNT_LS);