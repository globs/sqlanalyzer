DELETE FROM DELIVERY_VERSION WHERE VERSION='SP32';

INSERT INTO DELIVERY_VERSION (VERSION,CREATION_DT) VALUES 
('SP32',SYSDATE);