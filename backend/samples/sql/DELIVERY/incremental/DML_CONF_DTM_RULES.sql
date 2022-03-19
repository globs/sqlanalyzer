DELETE FROM CONF_DTM_RULES WHERE 1=1;

INSERT INTO CONF_DTM_RULES (RULE_CD,PERIODICITY_TYPE,PERIOD_TYPE_ID,IS_CASH_FLOW,IS_CU,PROJECTION_MONTH,CASHFLOW_DT,COEFF_AMOUNT) VALUES
 ('12MCF00',12,3,1,0,1,'YYYY-03-31',0.08333)
,('12MCF00',12,3,1,0,2,'YYYY-03-31',0.08333)
,('12MCF00',12,3,1,0,3,'YYYY-03-31',0.08333)
,('12MCF01',12,3,1,1,1,'YYYY-03-31',0.08333)
,('12MCF01',12,3,1,1,2,'YYYY-03-31',0.08333)
,('12MCF01',12,3,1,1,3,'YYYY-03-31',0.08333)
,('12MCF00',12,3,1,0,4,'YYYY-06-30',0.08333)
,('12MCF00',12,3,1,0,5,'YYYY-06-30',0.08333)
,('12MCF00',12,3,1,0,6,'YYYY-06-30',0.08333)
,('12MCF01',12,3,1,1,4,'YYYY-06-30',0.08333)
,('12MCF01',12,3,1,1,5,'YYYY-06-30',0.08333)
,('12MCF01',12,3,1,1,6,'YYYY-06-30',0.08333)
,('12MCF00',12,3,1,0,7,'YYYY-09-30',0.08333)
,('12MCF00',12,3,1,0,8,'YYYY-09-30',0.08333)
,('12MCF00',12,3,1,0,9,'YYYY-09-30',0.08333)
,('12MCF01',12,3,1,1,7,'YYYY-09-30',0.08333)
,('12MCF01',12,3,1,1,8,'YYYY-09-30',0.08333)
,('12MCF01',12,3,1,1,9,'YYYY-09-30',0.08333)
,('12MCF00',12,3,1,0,10,'YYYY-12-31',0.08333)
,('12MCF00',12,3,1,0,11,'YYYY-12-31',0.08333)
,('12MCF00',12,3,1,0,12,'YYYY-12-31',0.08333)
,('12MCF01',12,3,1,1,10,'YYYY-12-31',0.08333)
,('12MCF01',12,3,1,1,11,'YYYY-12-31',0.08333)
,('12MCF01',12,3,1,1,12,'YYYY-12-31',0.08333)
,('10MCF00',10,3,1,0,1,'YYYY-03-31',1.00)
,('10MCF00',10,3,1,0,2,'YYYY-03-31',1.00)
,('10MCF00',10,3,1,0,3,'YYYY-03-31',1.00)
,('10MCF00',10,3,1,0,4,'YYYY-06-30',1.00)
,('10MCF00',10,3,1,0,5,'YYYY-06-30',1.00)
,('10MCF00',10,3,1,0,6,'YYYY-06-30',1.00)
,('10MCF00',10,3,1,0,7,'YYYY-09-30',1.00)
,('10MCF00',10,3,1,0,8,'YYYY-09-30',1.00)
,('10MCF00',10,3,1,0,9,'YYYY-09-30',1.00)
,('10MCF00',10,3,1,0,10,'YYYY-12-31',1.00)
,('10MCF00',10,3,1,0,11,'YYYY-12-31',1.00)
,('10MCF00',10,3,1,0,12,'YYYY-12-31',1.00)
,('10MCF01',10,3,1,1,1,'YYYY-03-31',1.00)
,('10MCF01',10,3,1,1,2,'YYYY-03-31',1.00)
,('10MCF01',10,3,1,1,3,'YYYY-03-31',1.00)
,('10MCF01',10,3,1,1,4,'YYYY-06-30',1.00)
,('10MCF01',10,3,1,1,5,'YYYY-06-30',1.00)
,('10MCF01',10,3,1,1,6,'YYYY-06-30',1.00)
,('10MCF01',10,3,1,1,7,'YYYY-09-30',1.00)
,('10MCF01',10,3,1,1,8,'YYYY-09-30',1.00)
,('10MCF01',10,3,1,1,9,'YYYY-09-30',1.00)
,('10MCF01',10,3,1,1,10,'YYYY-12-31',1.00)
,('10MCF01',10,3,1,1,11,'YYYY-12-31',1.00)
,('10MCF01',10,3,1,1,12,'YYYY-12-31',1.00)
,('11QCF00',11,2,1,0,3,'YYYY-01-31',0.33333)
,('11QCF00',11,2,1,0,3,'YYYY-03-31',0.33333)
,('11QCF00',11,2,1,0,6,'YYYY-04-30',0.33333)
,('11QCF00',11,2,1,0,6,'YYYY-05-31',0.33333)
,('11QCF00',11,2,1,0,6,'YYYY-06-30',0.33333)
,('11QCF00',11,2,1,0,9,'YYYY-07-31',0.33333)
,('11QCF00',11,2,1,0,9,'YYYY-08-31',0.33333)
,('11QCF00',11,2,1,0,9,'YYYY-09-30',0.33333)
,('11QCF00',11,2,1,0,12,'YYYY-10-31',0.33333)
,('11QCF00',11,2,1,0,12,'YYYY-11-30',0.33333)
,('11QCF00',11,2,1,0,12,'YYYY-12-31',0.33333)
,('11QCF00',11,2,1,0,3,'YYYY-02-01',0.33333)
,('11QCF01',11,2,1,1,3,'YYYY-03-31',1.00)
,('11QCF01',11,2,1,1,6,'YYYY-06-30',1.00)
,('11QCF01',11,2,1,1,9,'YYYY-09-30',1.00)
,('11QCF01',11,2,1,1,12,'YYYY-12-31',1.00)
,('11MBS00',11,3,0,0,1,'YYYY-01-31',1.00)
,('11MBS00',11,3,0,0,3,'YYYY-03-31',1.00)
,('11MBS01',11,3,0,1,3,'YYYY-03-31',1.00)
,('11MBS00',11,3,0,0,4,'YYYY-04-30',1.00)
,('11MBS00',11,3,0,0,5,'YYYY-05-31',1.00)
,('11MBS00',11,3,0,0,6,'YYYY-06-30',1.00)
,('11MBS01',11,3,0,1,6,'YYYY-06-30',1.00)
,('11MBS00',11,3,0,0,7,'YYYY-07-31',1.00)
,('11MBS00',11,3,0,0,8,'YYYY-08-31',1.00)
,('11MBS00',11,3,0,0,9,'YYYY-09-30',1.00)
,('11MBS01',11,3,0,1,9,'YYYY-09-30',1.00)
,('11MBS00',11,3,0,0,10,'YYYY-10-31',1.00)
,('11MBS00',11,3,0,0,11,'YYYY-11-30',1.00)
,('11MBS00',11,3,0,0,12,'YYYY-12-31',1.00)
,('11MBS01',11,3,0,1,12,'YYYY-12-31',1.00)
,('11MBS00',11,3,0,0,2,'YYYY-02-01',1.00)
,('20MCF10',20,3,1,0,1,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,2,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,3,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,4,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,5,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,6,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,7,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,8,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,9,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,10,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,11,'YYYY-12-31',1.00)
,('20MCF10',20,3,1,0,12,'YYYY-12-31',1.00)
,('20MCF11',20,3,1,1,1,'YYYY-03-31',1.00)
,('20MCF11',20,3,1,1,2,'YYYY-03-31',1.00)
,('20MCF11',20,3,1,1,3,'YYYY-03-31',1.00)
,('20MCF11',20,3,1,1,4,'YYYY-06-30',1.00)
,('20MCF11',20,3,1,1,5,'YYYY-06-30',1.00)
,('20MCF11',20,3,1,1,6,'YYYY-06-30',1.00)
,('20MCF11',20,3,1,1,7,'YYYY-09-30',1.00)
,('20MCF11',20,3,1,1,8,'YYYY-09-30',1.00)
,('20MCF11',20,3,1,1,9,'YYYY-09-30',1.00)
,('20MCF11',20,3,1,1,10,'YYYY-12-31',1.00)
,('20MCF11',20,3,1,1,11,'YYYY-12-31',1.00)
,('20MCF11',20,3,1,1,12,'YYYY-12-31',1.00)
,('12QCF00',12,2,1,0,3,'YYYY-03-31',0.08333)
,('12QCF01',12,2,1,1,3,'YYYY-03-31',0.08333)
,('12QCF00',12,2,1,0,6,'YYYY-06-30',0.08333)
,('12QCF01',12,2,1,1,6,'YYYY-06-30',0.08333)
,('12QCF00',12,2,1,0,9,'YYYY-09-30',0.08333)
,('12QCF01',12,2,1,1,9,'YYYY-09-30',0.08333)
,('12QCF00',12,2,1,0,12,'YYYY-12-31',0.08333)
,('12QCF01',12,2,1,1,12,'YYYY-12-31',0.08333)
,('20QCF10',20,2,1,0,3,'YYYY-12-31',1.00)
,('20QCF10',20,2,1,0,6,'YYYY-12-31',1.00)
,('20QCF10',20,2,1,0,9,'YYYY-12-31',1.00)
,('20QCF10',20,2,1,0,12,'YYYY-12-31',1.00)
,('20QCF11',20,2,1,1,3,'YYYY-03-31',1.00)
,('20QCF11',20,2,1,1,6,'YYYY-06-30',1.00)
,('20QCF11',20,2,1,1,9,'YYYY-09-30',1.00)
,('20QCF11',20,2,1,1,12,'YYYY-12-31',1.00)
,('12QBS00',12,2,0,0,3,'YYYY-03-31',0.08333)
,('12QBS01',12,2,0,1,3,'YYYY-03-31',0.08333)
,('12QBS00',12,2,0,0,6,'YYYY-06-30',0.08333)
,('12QBS01',12,2,0,1,6,'YYYY-06-30',0.08333)
,('12QBS00',12,2,0,0,9,'YYYY-09-30',0.08333)
,('12QBS01',12,2,0,1,9,'YYYY-09-30',0.08333)
,('12QBS00',12,2,0,0,12,'YYYY-12-31',0.08333)
,('12QBS01',12,2,0,1,12,'YYYY-12-31',0.08333)
,('20YBS10',20,1,0,0,12,'YYYY-12-31',1.00)
,('20YCF10',20,1,1,0,12,'YYYY-12-31',1.00)
,('20YCF11',20,1,1,1,12,'YYYY-03-31',0.25)
,('20YCF11',20,1,1,1,12,'YYYY-06-30',0.25)
,('20YCF11',20,1,1,1,12,'YYYY-09-30',0.25)
,('20YCF11',20,1,1,1,12,'YYYY-12-31',0.25)
,('10QCF00',10,2,1,0,3,'YYYY-03-31',1.00)
,('10QCF00',10,2,1,0,6,'YYYY-06-30',1.00)
,('10QCF00',10,2,1,0,9,'YYYY-09-30',1.00)
,('10QCF00',10,2,1,0,12,'YYYY-12-31',1.00)
,('10QCF01',10,2,1,1,3,'YYYY-03-31',1.00)
,('10QCF01',10,2,1,1,6,'YYYY-06-30',1.00)
,('10QCF01',10,2,1,1,9,'YYYY-09-30',1.00)
,('10QCF01',10,2,1,1,12,'YYYY-12-31',1.00)
,('11MCF00',11,3,1,0,1,'YYYY-01-31',1.00)
,('11MCF00',11,3,1,0,3,'YYYY-03-31',1.00)
,('11MCF01',11,3,1,1,3,'YYYY-03-31',1.00)
,('11MCF00',11,3,1,0,4,'YYYY-04-30',1.00)
,('11MCF00',11,3,1,0,5,'YYYY-05-31',1.00)
,('11MCF00',11,3,1,0,6,'YYYY-06-30',1.00)
,('11MCF01',11,3,1,1,6,'YYYY-06-30',1.00)
,('11MCF00',11,3,1,0,7,'YYYY-07-31',1.00)
,('11MCF00',11,3,1,0,8,'YYYY-08-31',1.00)
,('11MCF00',11,3,1,0,9,'YYYY-09-30',1.00)
,('11MCF01',11,3,1,1,9,'YYYY-09-30',1.00)
,('11MCF00',11,3,1,0,10,'YYYY-10-31',1.00)
,('11MCF00',11,3,1,0,11,'YYYY-11-30',1.00)
,('11MCF00',11,3,1,0,12,'YYYY-12-31',1.00)
,('11MCF01',11,3,1,1,12,'YYYY-12-31',1.00)
,('12YCF00',12,1,1,0,12,'YYYY-03-31',0.08333)
,('12YCF01',12,1,1,1,12,'YYYY-03-31',0.08333)
,('12YCF00',12,1,1,0,12,'YYYY-06-30',0.08333)
,('12YCF01',12,1,1,1,12,'YYYY-06-30',0.08333)
,('12YCF00',12,1,1,0,12,'YYYY-09-30',0.08333)
,('12YCF01',12,1,1,1,12,'YYYY-09-30',0.08333)
,('12YCF00',12,1,1,0,12,'YYYY-12-31',0.08333)
,('12YCF01',12,1,1,1,12,'YYYY-12-31',0.08333)
,('11MCF00',11,3,1,0,2,'YYYY-02-01',1.00)
,('11MCF01',11,3,1,1,1,'YYYY-03-31',1.00)
,('11MCF01',11,3,1,1,2,'YYYY-03-31',1.00)
,('11MCF01',11,3,1,1,4,'YYYY-06-30',1.00)
,('11MCF01',11,3,1,1,5,'YYYY-06-30',1.00)
,('11MCF01',11,3,1,1,7,'YYYY-09-30',1.00)
,('11MCF01',11,3,1,1,8,'YYYY-09-30',1.00)
,('11MCF01',11,3,1,1,10,'YYYY-12-31',1.00)
,('11MCF01',11,3,1,1,11,'YYYY-12-31',1.00)
,('20QBS10',20,2,0,0,12,'YYYY-12-31',1.00)
,('20QBS11',20,2,0,1,3,'YYYY-03-31',1.00)
,('20QBS11',20,2,0,1,6,'YYYY-06-30',1.00)
,('20QBS11',20,2,0,1,9,'YYYY-09-30',1.00)
,('20QBS11',20,2,0,1,12,'YYYY-12-31',1.00)
,('10QBS00',10,2,0,0,3,'YYYY-03-31',1.00)
,('10QBS00',10,2,0,0,6,'YYYY-06-30',1.00)
,('10QBS00',10,2,0,0,9,'YYYY-09-30',1.00)
,('10QBS00',10,2,0,0,12,'YYYY-12-31',1.00)
,('10QBS01',10,2,0,1,3,'YYYY-03-31',1.00)
,('10QBS01',10,2,0,1,6,'YYYY-06-30',1.00)
,('10QBS01',10,2,0,1,9,'YYYY-09-30',1.00)
,('10QBS01',10,2,0,1,12,'YYYY-12-31',1.00)
,('10YCF00',10,1,1,0,12,'YYYY-03-31',0.25)
,('10YCF00',10,1,1,0,12,'YYYY-06-30',0.25)
,('10YCF00',10,1,1,0,12,'YYYY-09-30',0.25)
,('10YCF00',10,1,1,0,12,'YYYY-12-31',0.25)
,('10YCF01',10,1,1,1,12,'YYYY-03-31',0.25)
,('10YCF01',10,1,1,1,12,'YYYY-06-30',0.25)
,('10YCF01',10,1,1,1,12,'YYYY-09-30',0.25)
,('10YCF01',10,1,1,1,12,'YYYY-12-31',0.25)
,('10MBS00',10,3,0,0,3,'YYYY-03-31',1.00)
,('10MBS00',10,3,0,0,6,'YYYY-06-30',1.00)
,('10MBS00',10,3,0,0,9,'YYYY-09-30',1.00)
,('10MBS00',10,3,0,0,12,'YYYY-12-31',1.00)
,('10MBS01',10,3,0,1,3,'YYYY-03-31',1.00)
,('10MBS01',10,3,0,1,6,'YYYY-06-30',1.00)
,('10MBS01',10,3,0,1,9,'YYYY-09-30',1.00)
,('10MBS01',10,3,0,1,12,'YYYY-12-31',1.00)
,('11QBS01',11,2,0,1,3,'YYYY-03-31',1.00)
,('11QBS01',11,2,0,1,6,'YYYY-06-30',1.00)
,('11QBS01',11,2,0,1,9,'YYYY-09-30',1.00)
,('11QBS01',11,2,0,1,12,'YYYY-12-31',1.00)
,('20MBS10',20,3,0,0,12,'YYYY-12-31',1.00)
,('20MBS11',20,3,0,1,3,'YYYY-03-31',1.00)
,('20MBS11',20,3,0,1,6,'YYYY-06-30',1.00)
,('20MBS11',20,3,0,1,9,'YYYY-09-30',1.00)
,('20MBS11',20,3,0,1,12,'YYYY-12-31',1.00)
,('12MBS00',12,3,0,0,3,'YYYY-03-31',0.08333)
,('12MBS01',12,3,0,1,3,'YYYY-03-31',0.08333)
,('12MBS00',12,3,0,0,6,'YYYY-06-30',0.08333)
,('12MBS01',12,3,0,1,6,'YYYY-06-30',0.08333)
,('12MBS00',12,3,0,0,9,'YYYY-09-30',0.08333)
,('12MBS01',12,3,0,1,9,'YYYY-09-30',0.08333)
,('12MBS00',12,3,0,0,12,'YYYY-12-31',0.08333)
,('12MBS01',12,3,0,1,12,'YYYY-12-31',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-01-31',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-03-31',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-04-30',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-05-31',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-06-30',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-07-31',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-08-31',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-09-30',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-10-31',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-11-30',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-12-31',0.08333)
,('11YCF00',11,1,1,0,12,'YYYY-02-01',0.08333)
,('11YCF01',11,1,1,1,12,'YYYY-03-31',0.25)
,('11YCF01',11,1,1,1,12,'YYYY-06-30',0.25)
,('11YCF01',11,1,1,1,12,'YYYY-09-30',0.25)
,('11YCF01',11,1,1,1,12,'YYYY-12-31',0.25);