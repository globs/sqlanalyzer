DELETE FROM DELIVERY_VERSION WHERE VERSION='SP38';

INSERT INTO DELIVERY_VERSION (VERSION,CREATION_DT) VALUES 
('SP38',SYSDATE);