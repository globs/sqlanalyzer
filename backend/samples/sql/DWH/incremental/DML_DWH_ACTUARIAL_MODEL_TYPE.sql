DELETE FROM DWH_ACTUARIAL_MODEL_TYPE;

INSERT INTO DWH_ACTUARIAL_MODEL_TYPE (ID,CODE,NAME,CREATED_DATE)
VALUES (1,'SERIATIM','Seriatim',SYSDATE);
INSERT INTO DWH_ACTUARIAL_MODEL_TYPE (ID,CODE,NAME,CREATED_DATE)
VALUES (2,'NON_SERIATIM','Non Seriatim',SYSDATE);