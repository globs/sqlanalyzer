DELETE FROM EVENT_TRIGGER WHERE EVENT_CODE IN ('EVT_NAT_CASHFLOWS_VALIDATED','EVT_SAT_CASHFLOWS_VALIDATED'
,'EVT_ANZ_CASHFLOWS_VALIDATED','EVT_CHINA_CASHFLOWS_VALIDATED');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_NAT_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "NORTHASIA",
"eventCode" : "EVT_NAT_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/apacpropht/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_SAT_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "SOUTHASIA",
"eventCode" : "EVT_SAT_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/apacpropht/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_ANZ_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "AUSNZ",
"eventCode" : "EVT_ANZ_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/apacpropht/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_CHINA_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "CHINA",
"eventCode" : "EVT_CHINA_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/apacpropht/in",
"dataFileList" : []
}');