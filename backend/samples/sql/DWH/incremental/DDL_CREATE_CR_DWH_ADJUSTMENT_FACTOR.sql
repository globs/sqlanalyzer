DROP TABLE DWH_ADJUSTMENT_FACTOR IF EXISTS;

CREATE TABLE DWH_ADJUSTMENT_FACTOR (
   REPORTING_BASIS_ID   SMALLINT,
   CLOSING_DATE         DATE             
      NOT NULL,
   PARAMETER_TYPE_ID    SMALLINT              
      NOT NULL,
   SEGMENT_TYPE_ID      SMALLINT,
   SEGMENT_CODE         VARCHAR(16),
   CONTRACT_NATURE_CODE CHAR(1),
   SUBSIDIARY_CODE      SMALLINT,
   SUBLEDGER_CODE       SMALLINT,
   LOB_CODE             CHAR(2),
   DOMAIN_CODE          VARCHAR(16),
   PLAN_CATEGORY_ID     SMALLINT,
   UNDERWRITING_YEAR    SMALLINT,
   CURRENCY_CODE        CHAR(3),
   FACTOR_TYPE_ID       SMALLINT,
   FACTOR               DECFLOAT              
      NOT NULL,
   HASH_KEY             VARBINARY(64)         
      NOT NULL,
   VALID_FROM           TIMESTAMP             
      NOT NULL,
   VALID_TO             TIMESTAMP             
      NOT NULL,
   PIVOT_KEY            VARBINARY(64)         
      NOT NULL,
   SUPP_DATE            TIMESTAMP,
   CREATED_REQUEST_ID   BIGINT,
   MODIFIED_REQUEST_ID  BIGINT,
   DELETED_REQUEST_ID   BIGINT,
   CONSTRAINT PK_DWH_ADJUSTMENT_FACTOR PRIMARY KEY (HASH_KEY, 
VALID_FROM)  
)
 in TBS_<env>  DISTRIBUTE BY hash(HASH_KEY);

COMMENT ON TABLE DWH_ADJUSTMENT_FACTOR IS
'Adjustment Factor  is a combination of Parameter Type and Adjustment  F
actor Type. Each Parameter can have one or more Adjustment factor. ';

COMMENT ON COLUMN DWH_ADJUSTMENT_FACTOR.SEGMENT_CODE IS
'Code of the segmentation is the short label of the segmenation';

COMMENT ON COLUMN DWH_ADJUSTMENT_FACTOR.SUBSIDIARY_CODE IS
'Code filiale';

COMMENT ON COLUMN DWH_ADJUSTMENT_FACTOR.SUBLEDGER_CODE IS
'Code �tablissement';

COMMENT ON COLUMN DWH_ADJUSTMENT_FACTOR.CURRENCY_CODE IS
'ISO 4217 currency code';

ALTER TABLE DWH_ADJUSTMENT_FACTOR
   ADD CONSTRAINT FK_ADJUSTMENT_FACTOR_REPORTING_BASIS FOREIGN KEY 
(REPORTING_BASIS_ID)
      REFERENCES DWH_REPORTING_BASIS (ID)
      ON DELETE RESTRICT
      NOT ENFORCED;

ALTER TABLE DWH_ADJUSTMENT_FACTOR
   ADD CONSTRAINT FK_ADJUSTMENT_FACTOR_SEGMENT_TYPE FOREIGN KEY 
(SEGMENT_TYPE_ID)
      REFERENCES DWH_SEGMENT_TYPE (ID)
      ON DELETE RESTRICT
      NOT ENFORCED;

ALTER TABLE DWH_ADJUSTMENT_FACTOR
   ADD CONSTRAINT FK_ADJUSTMENT_FACTOR_TYPE FOREIGN KEY (FACTOR_TYPE_ID)

      REFERENCES DWH_ADJUSTMENT_FACTOR_TYPE (ID)
      ON DELETE RESTRICT
      NOT ENFORCED;

ALTER TABLE DWH_ADJUSTMENT_FACTOR
   ADD CONSTRAINT FK_RA_PARAMETER_PLAN_CATEGORY FOREIGN KEY 
(PLAN_CATEGORY_ID)
      REFERENCES DWH_PLAN_CATEGORY (ID)
      ON DELETE RESTRICT
      NOT ENFORCED;

ALTER TABLE DWH_ADJUSTMENT_FACTOR
   ADD CONSTRAINT FK_RA_PARAMETER_TYPE FOREIGN KEY (PARAMETER_TYPE_ID)
      REFERENCES DWH_PARAMETER_TYPE (ID)
      ON DELETE RESTRICT
      NOT ENFORCED;