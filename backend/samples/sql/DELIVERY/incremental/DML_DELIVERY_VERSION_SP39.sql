DELETE FROM DELIVERY_VERSION WHERE VERSION='SP39';

INSERT INTO DELIVERY_VERSION (VERSION,CREATION_DT) VALUES 
('SP39',SYSDATE);