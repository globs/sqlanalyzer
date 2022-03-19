SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_FINANCIAL_SOLUTION AS
SELECT
    CAST("Financial_Solution" AS CHAR(5)) AS "Financial_Solution",
    CAST("Financial_Solution_Description" AS VARCHAR(32)) AS "Financial_Solution_Description"
FROM
    (
    SELECT
        TUWS.LIFFINSOLA_CF AS "Financial_Solution",
        TUWS.LIFFINSOLA_LL AS "Financial_Solution_Description"
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
        AND TUWS.supp_d = '9999-12-31'
        AND TUWS.END_D = '9999-12-31'
    GROUP BY
        TUWS.LIFFINSOLA_CF,
        TUWS.LIFFINSOLA_LL
UNION
    SELECT
        TUWR.LIFFINSOLR_CF AS "Financial_Solution",
        TUWR.LIFFINSOLR_LL AS "Financial_Solution_Description"
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
        AND TUWR.supp_d = '9999-12-31'
        AND TUWR.END_D = '9999-12-31'
    GROUP BY
        TUWR.LIFFINSOLR_CF,
        TUWR.LIFFINSOLR_LL);