DROP TABLE DWH_TDATAMART_PAPM_CLAIMS  IF EXISTS;
 
CREATE TABLE DWH_TDATAMART_PAPM_CLAIMS  (
		  "Validity Date" VARCHAR(10) NOT NULL , 
		  "TL - Balance Sheet Year" CHAR(4) NOT NULL , 
		  "Quarter" CHAR(1) NOT NULL , 
		  "Gaap Code" SMALLINT NOT NULL , 
		  "TL - DISCOUNT" CHAR(1) NOT NULL , 
		  "TL - Subsidiary" VARCHAR(2) NOT NULL , 
		  "TL - Subsidiary Ledger" VARCHAR(2) NOT NULL , 
		  "Contract Nature" CHAR(1) , 
		  "Geographical Zone" VARCHAR(6) , 
		  "Loyds_Flags" CHAR(1) , 
		  "TL - Currency" CHAR(3) , 
		  "TL - Acc. Amount" DECIMAL(31,3) )   
		 DISTRIBUTE BY RANDOM IN TBS_<env>  
		 ORGANIZE BY COLUMN  ;