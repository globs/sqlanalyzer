SET SCHEMA STAGING_<env>;

DROP TABLE WRK_DIP_LRM_TREATY_ATTR IF EXISTS;

CREATE TABLE WRK_DIP_LRM_TREATY_ATTR (
	ID BIGINT NOT NULL,
	"Treaty_Number" CHAR(9) NOT NULL,
	"Section_Number" SMALLINT NOT NULL,
	"Treaty_Section_Name" VARCHAR(64),
	"Policy_Underwriting_Year" SMALLINT,
	"Subsidiary_Code" SMALLINT,
	"Ledger_Code" SMALLINT,
	"Region_Code" VARCHAR(3),
	"Treaty_Life_Characteristic_Code" VARCHAR(2),
	"Financing_Type_Code" VARCHAR(5),
	"IAS39_Code" INTEGER,
	"USGAAP_Code" INTEGER,
	"Coinsurance_Code" VARCHAR(2),
	"Type_Of_Business_Code" INTEGER,
	"Omega_LoB_Code" CHAR(2),
	"Guarantee_Code" CHAR(3),
	"Policy_Type_Code" VARCHAR(3),
	"Nature_Code" VARCHAR(4),
	"Basis_Type" VARCHAR(3),
	"Counter_Party_Code" VARCHAR(4000),
	"Actuarial_Model_Type_Code" SMALLINT,
	"Client_Code" INTEGER,
	"Term_Type_Code" SMALLINT,
	"Business_Maturity_Code" VARCHAR(32) NOT NULL,
	"Business_Maturity_Flag" VARCHAR(1),
	"Market_Unit_Code" SMALLINT,
	"Gross_Assumed_Treaty_Number" CHAR(9),
	"Gross_Assumed_Section_Number" SMALLINT,
	"Closing_Date" DATE NOT NULL,
	"Reporting_Basis_Code" VARCHAR(32) NOT NULL,
	"IFRS17_Cohort" VARCHAR(150),
	"IFRS17_Portfolio" VARCHAR(100),
	"IFRS17_Sub_Portfolio" VARCHAR(100),
	"Transition_Mode" VARCHAR(100),
	"Initial_Profitability" VARCHAR(100),
	"Annual_Cohort" VARCHAR(100),
	"Split" VARCHAR(32)
)
ORGANIZE BY COLUMN IN TBS_<env> DISTRIBUTE ON RANDOM;
