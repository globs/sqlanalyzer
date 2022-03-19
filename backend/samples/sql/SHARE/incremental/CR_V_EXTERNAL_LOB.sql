SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_EXTERNAL_LOB AS
SELECT
    CAST("External_Lob" AS CHAR(5)) AS "External_Lob",
    CAST("External_Lob_Description" AS CHAR(32)) AS "External_Lob_Description"
FROM
    (
    SELECT
        TUWS.LIFLOBA_CF AS "External_Lob",
        TUWS.LIFLOBA_LL AS "External_Lob_Description"
    FROM
        BI_<env>.TUWSEC TUWS
    WHERE
        TUWS.ACTIV2_CF = '400'
        AND TUWS.SUBLEDGER_CF NOT IN ('05-01',
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
        AND TUWS.Supp_d = '9999-12-31'
        AND TUWS.END_D = '9999-12-31'
    GROUP BY
        TUWS.LIFLOBA_CF,
        TUWS.LIFLOBA_LL
UNION
    SELECT
        TUWR.LIFLOBR_CF AS "External_Lob",
        TUWR.LIFLOBR_LL AS "External_Lob_Description"
    FROM
        BI_<env>.TUWRETSEC TUWR
    WHERE
        TUWR.ACTIV2_CF = '400'
        AND TUWR.SUBLEDGER_CF NOT IN ('05-01',
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
        AND TUWR.Supp_d = '9999-12-31'
        AND TUWR.END_D = '9999-12-31'
    GROUP BY
        TUWR.LIFLOBR_CF,
        TUWR.LIFLOBR_LL);