DELETE FROM REFCODL WHERE REFCODL_ID IN (22, 23, 24);
INSERT INTO REFCODL (REFCODL_ID, REFCOD_ID, CREUSR_CF, LSTUPDUSR_CF, LANGUAGE_CODE, REFERENCE_CODE_LS, REFERENCE_CODE_LM, REFERENCE_CODE_LL, CREATION_DT, LAST_UPDATE_DT ) VALUES (22, 22, 'GE21','GE21','E', 'SAS_CD', 'SAS_ENTITY_CODE', NULL, SYSDATE, SYSDATE);
INSERT INTO REFCODL (REFCODL_ID, REFCOD_ID, CREUSR_CF, LSTUPDUSR_CF, LANGUAGE_CODE, REFERENCE_CODE_LS, REFERENCE_CODE_LM, REFERENCE_CODE_LL, CREATION_DT, LAST_UPDATE_DT ) VALUES (23, 23, 'GE21','GE21','E', 'WORKGRP_CD', 'WORK_GROUP_CODE', NULL, SYSDATE, SYSDATE);
INSERT INTO REFCODL (REFCODL_ID, REFCOD_ID, CREUSR_CF, LSTUPDUSR_CF, LANGUAGE_CODE, REFERENCE_CODE_LS, REFERENCE_CODE_LM, REFERENCE_CODE_LL, CREATION_DT, LAST_UPDATE_DT ) VALUES (24, 24, 'GE21','GE21','E', 'INTVL_CD', 'INTERVAL_CODE', NULL, SYSDATE, SYSDATE);