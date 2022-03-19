SET SCHEMA SHARE_<env>;

CREATE OR REPLACE
VIEW V_PRODUCT_LINE AS
SELECT
    CAST("Product_Line" AS VARCHAR(5)) AS "Product_Line",
    CAST("Product_Line_Description" AS VARCHAR(32)) AS "Product_Line_Description"
FROM
    (
    SELECT
        TUWS.LIFPRDLINA_CF AS "Product_Line",
        TUWS.LIFPRDLINA_LL AS "Product_Line_Description"
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
        TUWS.LIFPRDLINA_CF,
        TUWS.LIFPRDLINA_LL
UNION
    SELECT
        TUWR.LIFPRDLINR_CF AS "Product_Line",
        TUWR.LIFPRDLINR_LL AS "Product_Line_Description"
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
        TUWR.LIFPRDLINR_CF,
        TUWR.LIFPRDLINR_LL);