SET SQL_COMPAT='DB2'; 

DROP TABLE DWH_DISCOUNT_CURRENCY_MAPPING IF EXISTS;

CREATE TABLE DWH_DISCOUNT_CURRENCY_MAPPING (
   CURRENCY_CODE			   CHAR(3)			NOT NULL,
	DISCOUNT_CURRENCY_CODE	CHAR(3)			NOT NULL,
	LOB_CODE				      CHAR(2),
	HASH_KEY				      VARBINARY(64)	NOT NULL,
	VALID_FROM				   TIMESTAMP		NOT NULL,
	VALID_TO				      TIMESTAMP		NOT NULL,
	PIVOT_KEY				   VARBINARY(64)	NOT NULL,
	SUPP_DATE				   TIMESTAMP,
	CREATED_REQUEST_ID		BIGINT,
	MODIFIED_REQUEST_ID		BIGINT,
	DELETED_REQUEST_ID		BIGINT,
	CONSTRAINT PK_DISCOUNT_CURRENCY PRIMARY KEY (HASH_KEY, VALID_FROM) 
)
ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;

COMMENT ON COLUMN DWH_DISCOUNT_CURRENCY_MAPPING.CURRENCY_CODE IS
'ISO 4217 currency code';