CREATE OR REPLACE VIEW V_OMEGA_LKR (
SUBSIDIARY,
SUBLEDGER,
SEGMENT,
LOB,
CURRENCY_CODE,
CODE,
TYPE,
CLOSING_DATE,
Y1,
Y2,
Y3,
Y4,
Y5,
Y6,
Y7,
Y8,
Y9,
Y10,
Y11,
Y12,
Y13,
Y14,
Y15,
Y16,
Y17,
Y18,
Y19,
Y20,
Y21,
Y22,
Y23,
Y24,
Y25,
Y26,
Y27,
Y28,
Y29,
Y30,
Y31,
Y32,
Y33,
Y34,
Y35,
Y36,
Y37,
Y38,
Y39,
Y40,
Y41,
Y42,
Y43,
Y44,
Y45,
Y46,
Y47,
Y48,
Y49,
Y50,
Y51,
Y52,
Y53,
Y54,
Y55,
Y56,
Y57,
Y58,
Y59,
Y60,
Y61,
Y62,
Y63,
Y64,
Y65
) 
AS
(
SELECT ec.subsidiary_code AS SUBSIDIARY, ec.subledger_code AS SUBLEDGER, CAST('' AS VARCHAR(16)) AS segment,YEAR(ec.origin_closing_date)||'Q'||QUARTER(EC.origin_closing_date)||'ILL'||COALESCE(seg.seg_code,'x') AS LOB, ec.currency_code, b.code, 'LKR' AS TYPE, ec.closing_date AS CLOSING_DATE
        ,MAX(CASE WHEN ec.maturity=1 THEN ROUND(ec.rate,8) END) AS Y1
        ,MAX(CASE WHEN ec.maturity=2 THEN ROUND(ec.rate,8) END) AS Y2
        ,MAX(CASE WHEN ec.maturity=3 THEN ROUND(ec.rate,8) END) AS Y3
        ,MAX(CASE WHEN ec.maturity=4 THEN ROUND(ec.rate,8) END) AS Y4
        ,MAX(CASE WHEN ec.maturity=5 THEN ROUND(ec.rate,8) END) AS Y5
        ,MAX(CASE WHEN ec.maturity=6 THEN ROUND(ec.rate,8) END) AS Y6
        ,MAX(CASE WHEN ec.maturity=7 THEN ROUND(ec.rate,8) END) AS Y7
        ,MAX(CASE WHEN ec.maturity=8 THEN ROUND(ec.rate,8) END) AS Y8
        ,MAX(CASE WHEN ec.maturity=9 THEN ROUND(ec.rate,8) END) AS Y9
        ,MAX(CASE WHEN ec.maturity=10 THEN ROUND(ec.rate,8) END) AS Y10
        ,MAX(CASE WHEN ec.maturity=11 THEN ROUND(ec.rate,8) END) AS Y11
        ,MAX(CASE WHEN ec.maturity=12 THEN ROUND(ec.rate,8) END) AS Y12
        ,MAX(CASE WHEN ec.maturity=13 THEN ROUND(ec.rate,8) END) AS Y13
        ,MAX(CASE WHEN ec.maturity=14 THEN ROUND(ec.rate,8) END) AS Y14
        ,MAX(CASE WHEN ec.maturity=15 THEN ROUND(ec.rate,8) END) AS Y15
        ,MAX(CASE WHEN ec.maturity=16 THEN ROUND(ec.rate,8) END) AS Y16
        ,MAX(CASE WHEN ec.maturity=17 THEN ROUND(ec.rate,8) END) AS Y17
        ,MAX(CASE WHEN ec.maturity=18 THEN ROUND(ec.rate,8) END) AS Y18
        ,MAX(CASE WHEN ec.maturity=19 THEN ROUND(ec.rate,8) END) AS Y19
        ,MAX(CASE WHEN ec.maturity=20 THEN ROUND(ec.rate,8) END) AS Y20
        ,MAX(CASE WHEN ec.maturity=21 THEN ROUND(ec.rate,8) END) AS Y21
        ,MAX(CASE WHEN ec.maturity=22 THEN ROUND(ec.rate,8) END) AS Y22
        ,MAX(CASE WHEN ec.maturity=23 THEN ROUND(ec.rate,8) END) AS Y23
        ,MAX(CASE WHEN ec.maturity=24 THEN ROUND(ec.rate,8) END) AS Y24
        ,MAX(CASE WHEN ec.maturity=25 THEN ROUND(ec.rate,8) END) AS Y25
        ,MAX(CASE WHEN ec.maturity=26 THEN ROUND(ec.rate,8) END) AS Y26
        ,MAX(CASE WHEN ec.maturity=27 THEN ROUND(ec.rate,8) END) AS Y27
        ,MAX(CASE WHEN ec.maturity=28 THEN ROUND(ec.rate,8) END) AS Y28
        ,MAX(CASE WHEN ec.maturity=29 THEN ROUND(ec.rate,8) END) AS Y29
        ,MAX(CASE WHEN ec.maturity=30 THEN ROUND(ec.rate,8) END) AS Y30
        ,MAX(CASE WHEN ec.maturity=31 THEN ROUND(ec.rate,8) END) AS Y31
        ,MAX(CASE WHEN ec.maturity=32 THEN ROUND(ec.rate,8) END) AS Y32
        ,MAX(CASE WHEN ec.maturity=33 THEN ROUND(ec.rate,8) END) AS Y33
        ,MAX(CASE WHEN ec.maturity=34 THEN ROUND(ec.rate,8) END) AS Y34
        ,MAX(CASE WHEN ec.maturity=35 THEN ROUND(ec.rate,8) END) AS Y35
        ,MAX(CASE WHEN ec.maturity=36 THEN ROUND(ec.rate,8) END) AS Y36
        ,MAX(CASE WHEN ec.maturity=37 THEN ROUND(ec.rate,8) END) AS Y37
        ,MAX(CASE WHEN ec.maturity=38 THEN ROUND(ec.rate,8) END) AS Y38
        ,MAX(CASE WHEN ec.maturity=39 THEN ROUND(ec.rate,8) END) AS Y39
        ,MAX(CASE WHEN ec.maturity=40 THEN ROUND(ec.rate,8) END) AS Y40
        ,MAX(CASE WHEN ec.maturity=41 THEN ROUND(ec.rate,8) END) AS Y41
        ,MAX(CASE WHEN ec.maturity=42 THEN ROUND(ec.rate,8) END) AS Y42
        ,MAX(CASE WHEN ec.maturity=43 THEN ROUND(ec.rate,8) END) AS Y43
        ,MAX(CASE WHEN ec.maturity=44 THEN ROUND(ec.rate,8) END) AS Y44
        ,MAX(CASE WHEN ec.maturity=45 THEN ROUND(ec.rate,8) END) AS Y45
        ,MAX(CASE WHEN ec.maturity=46 THEN ROUND(ec.rate,8) END) AS Y46
        ,MAX(CASE WHEN ec.maturity=47 THEN ROUND(ec.rate,8) END) AS Y47
        ,MAX(CASE WHEN ec.maturity=48 THEN ROUND(ec.rate,8) END) AS Y48
        ,MAX(CASE WHEN ec.maturity=49 THEN ROUND(ec.rate,8) END) AS Y49
        ,MAX(CASE WHEN ec.maturity=50 THEN ROUND(ec.rate,8) END) AS Y50
        ,MAX(CASE WHEN ec.maturity=51 THEN ROUND(ec.rate,8) END) AS Y51
        ,MAX(CASE WHEN ec.maturity=52 THEN ROUND(ec.rate,8) END) AS Y52
        ,MAX(CASE WHEN ec.maturity=53 THEN ROUND(ec.rate,8) END) AS Y53
        ,MAX(CASE WHEN ec.maturity=54 THEN ROUND(ec.rate,8) END) AS Y54
        ,MAX(CASE WHEN ec.maturity=55 THEN ROUND(ec.rate,8) END) AS Y55
        ,MAX(CASE WHEN ec.maturity=56 THEN ROUND(ec.rate,8) END) AS Y56
        ,MAX(CASE WHEN ec.maturity=57 THEN ROUND(ec.rate,8) END) AS Y57
        ,MAX(CASE WHEN ec.maturity=58 THEN ROUND(ec.rate,8) END) AS Y58
        ,MAX(CASE WHEN ec.maturity=59 THEN ROUND(ec.rate,8) END) AS Y59
        ,MAX(CASE WHEN ec.maturity=60 THEN ROUND(ec.rate,8) END) AS Y60
        ,MAX(CASE WHEN ec.maturity=61 THEN ROUND(ec.rate,8) END) AS Y61
        ,MAX(CASE WHEN ec.maturity=62 THEN ROUND(ec.rate,8) END) AS Y62
        ,MAX(CASE WHEN ec.maturity=63 THEN ROUND(ec.rate,8) END) AS Y63
        ,MAX(CASE WHEN ec.maturity=64 THEN ROUND(ec.rate,8) END) AS Y64
        ,MAX(CASE WHEN ec.maturity=65 THEN ROUND(ec.rate,8) END) AS Y65
FROM BI_<env>.economic_rate ec
INNER JOIN BI_<env>.parameter_type pt ON pt.id = ec.parameter_type_id
INNER JOIN BI_<env>.REPORTING_BASIS b ON b.id = ec.reporting_basis_id
LEFT JOIN BI_<env>.segment_type s ON s.id = ec.segment_type_id
INNER JOIN BI_<env>.MATURITY_TYPE mt ON mt.id = ec.reporting_basis_id
INNER JOIN 
(
	SELECT C.sgmt_ll AS seg_ls, sgmt_ls AS seg_code
	FROM  BI_<env>.TSEGMENTATION B
	JOIN  BI_<env>.TSEGMT C
	  ON  B.SGT_NT    = C.SGT_NT
	 AND  B.SGTVER_NT = C.SGTVER_NT
	WHERE B.SGTTYP_NT = 78 
	AND B.SGTVER_NT = (SELECT MAX(SGTVER_NT) FROM BI_<env>.TSEGMENTATION WHERE SGTTYP_NT = 78)
	AND c.sgmt_ll LIKE 'ILL%'
) seg ON seg.seg_ls = 'ILL'||COALESCE(ec.segment_code,'x')
WHERE 
pt.code = 'FWD'
AND ec.lob_code IS NULL
AND ec.VALID_TO = '9999-12-31'
AND ec.SUPP_DATE = '9999-12-31'
GROUP BY ec.closing_date, ec.subsidiary_code, ec.subledger_code, YEAR(ec.origin_closing_date)||'Q'||QUARTER(EC.origin_closing_date)||'ILL'||COALESCE(seg.seg_code,'x'), ec.currency_code, b.code
);