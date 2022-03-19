DROP TABLE DWH_PROJECTION_ALLOCATION IF EXISTS;

CREATE TABLE DWH_PROJECTION_ALLOCATION (
   CONTRACT_NUMBER      CHAR(9)               
      NOT NULL,
   SECTION_NUMBER       SMALLINT              
      NOT NULL,
   ASSUMED_CONTRACT_NUMBER CHAR(9),
   ASSUMED_SECTION_NUMBER SMALLINT,
   ALLOCATION_STATUS_ID BIGINT,
   REPORTING_BASIS_ID   SMALLINT,
   CLOSING_DATE         DATE,
   LEVEL_OF_ANALYSIS_ID INTEGER,
   CREATED_REQUEST_ID   BIGINT                
      NOT NULL,
   MODIFIED_REQUEST_ID  BIGINT,
   DELETED_REQUEST_ID   BIGINT,
   HASH_KEY             VARBINARY(64)         
      NOT NULL,
   VALID_FROM           TIMESTAMP             
      NOT NULL,
   VALID_TO             TIMESTAMP             
      NOT NULL,
   PIVOT_KEY            VARBINARY(64)         
      NOT NULL,
   SUPP_DATE            TIMESTAMP             
      NOT NULL,
   CONSTRAINT PK_RA_ALLOCATION PRIMARY KEY (HASH_KEY, VALID_FROM)  
)
 in TBS_<env>  DISTRIBUTE BY hash(HASH_KEY);

COMMENT ON TABLE DWH_PROJECTION_ALLOCATION IS
'Allocation tracking for RA or Cost of Capital ';

COMMENT ON COLUMN DWH_PROJECTION_ALLOCATION.SUPP_DATE IS
'Date when the record is deleted. ';

ALTER TABLE DWH_PROJECTION_ALLOCATION
   ADD CONSTRAINT FK_PROJECTION_ALLOCATION FOREIGN KEY (HASH_KEY,VALID_FROM )
      REFERENCES DWH_PROJECTION (HASH_KEY, VALID_FROM)
      ON DELETE RESTRICT
      NOT ENFORCED;

ALTER TABLE DWH_PROJECTION_ALLOCATION
   ADD CONSTRAINT FK_REFERENCE_17 FOREIGN KEY (LEVEL_OF_ANALYSIS_ID)
      REFERENCES DWH_LEVEL_OF_ANALYSIS (ID)
      ON DELETE RESTRICT
      NOT ENFORCED;

ALTER TABLE DWH_PROJECTION_ALLOCATION
   ADD CONSTRAINT FK_REFERENCE_18 FOREIGN KEY (REPORTING_BASIS_ID)
      REFERENCES DWH_REPORTING_BASIS (ID)
      ON DELETE RESTRICT
      NOT ENFORCED;