DELETE FROM DWH_INTERVAL_TYPE;

INSERT INTO DWH_INTERVAL_TYPE (ID,CODE,NAME,CREATED_DATE)
VALUES(1,'Annually','Annually',SYSDATE);
INSERT INTO DWH_INTERVAL_TYPE (ID,CODE,NAME,CREATED_DATE)
VALUES(2,'Quarterly','Quarterly',SYSDATE);
INSERT INTO DWH_INTERVAL_TYPE (ID,CODE,NAME,CREATED_DATE)
VALUES(3,'Monthly','Monthly',SYSDATE);