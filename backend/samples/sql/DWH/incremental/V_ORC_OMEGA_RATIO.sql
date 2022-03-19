CREATE OR REPLACE
VIEW V_OMEGA_RATIO (
SUBSIDIARY,
SUBLEDGER,
MARKET,
NATURE,
NORM,
ACQUISITION,
MAINTENANCE,
CLOSING_DATE
) AS
(SELECT f.subsidiary_code AS SUBSIDIARY, f.subledger_code AS SUBLEDGER, f.segment_code AS MARKET, 
	f.contract_nature_code AS NATURE, b.code AS NORM,
	DECIMAL(MAX(CASE WHEN pf.code = 'ACQUISITION_COST_FACTOR' then f.factor END),31,8) AS ACQUISITION,
	DECIMAL(MAX(CASE WHEN pf.code = 'MAINTENANCE_COST_FACTOR' then f.factor END),31,8) AS MAINTENANCE, f.closing_date
FROM BI_<env>.adjustment_factor f
INNER JOIN BI_<env>.parameter_type pt ON pt.id = f.parameter_type_id
INNER JOIN BI_<env>.segment_type s ON s.id = f.segment_type_id
inner JOIN BI_<env>.REPORTING_BASIS b ON b.id = f.reporting_basis_id
LEFT OUTER JOIN BI_<env>.ADJUSTMENT_FACTOR_TYPE  pf ON (f.FACTOR_TYPE_ID  = pf.id)
WHERE pt.CODE = 'AIE'
AND pf.CODE IN ('ACQUISITION_COST_FACTOR','MAINTENANCE_COST_FACTOR')
AND f.valid_to = '9999-12-31'
AND f.supp_date = '9999-12-31'
GROUP BY f.closing_date,f.valid_from, f.valid_to, f.subsidiary_code, f.subledger_code, f.segment_code, f.contract_nature_code, b.code
);