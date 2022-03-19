DELETE FROM EVENT_TRIGGER;

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_US_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "US",
"eventCode" : "EVT_US_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/uspropht/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_CANADA_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "CANADA",
"eventCode" : "EVT_CANADA_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/canaxis/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_FRGE_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "FRGE",
"eventCode" : "EVT_FRGE_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/gemmind/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_UK_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "UK",
"eventCode" : "EVT_UK_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/ukpropht/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_APAC_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "APAC",
"eventCode" : "EVT_APAC_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/apacpropht/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_MU_CASHFLOWS_VALIDATED', '{
"dipRunId" : null,
"cashflowMode": "FULL",
"cashflowRegion": "APAC",
"eventCode" : "EVT_MU_CASHFLOWS_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q32021",
"closingDate" : "2021-09-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/mupload/in",
"dataFileList" : []
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_SAS_DATA', '{
    "DIP_RUN_ID": null,
    "CSM_IDENTIFIER_ID": "test2",
    "EVENT_CODE": "EVT_DIP_TO_SAS_DATA",
    "MESSAGE": "Request",
    "REPORTING_DATE": "2021-06-30",
    "AS_OF_DATE": "2021-05-17 16:14:00",
    "RUN_TYPE": "I17G",
    "SAS_ENTITY": "SGL",
    "GEOGRAPHICAL_ZONE": "1_EMEA",
    "WORKGROUP": "TS_PATTERN",
    "SCENARIO_TYPE": "CLOSING",
	"SCENARIO_TYPE_ID": 1,
    "REQUEST_TYPE": "POSTING",
    "SENSITIVES_TYPE": "ESTIMATES",
    "CYCLE_ID": "",
    "FILES": [],
    "ERROR_MESSAGE": "",
    "INTERVAL": "SEMI-YEAR"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_LRM', '{
    "eventCode": "EVT_DIP_TO_LRM",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_LRM_TO_DIP_COC_VALIDATED', '{
"dipRunId" : null,
"lrmMode": "INCR",
"region": "LRM",
"eventCode" : "EVT_LRM_TO_DIP_COC_VALIDATED",
"closingType" : "I17G",
"closingPeriod" : "Q22021",
"closingDate" : "2021-06-30",
"scenarioType" : "CLOSING",
"runType" : "POSTING",
"fileDirectory" : "/ilias/in",
"dataFileList" : []
}');

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

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_DSC', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_DSC",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_LKR', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_LKR",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_FWD', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_FWD",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_RA', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_RA",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_DIP_TO_OMEGA_FOR_RATIO', '{
    "eventCode": "EVT_DIP_TO_OMEGA_FOR_RATIO",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_I17_ATTRIBUTES_CONTROL', '{
    "eventCode": "EVT_I17_ATTRIBUTES_CONTROL",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_I17_PRODUCT_CONTROL', '{
    "eventCode": "EVT_I17_PRODUCT_CONTROL",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_CODES_CONTROL', '{
    "eventCode": "EVT_CODES_CONTROL",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');

INSERT INTO EVENT_TRIGGER (EVENT_CODE, JSON_PARAM) VALUES('EVT_OMEGA_AE_CONTROL', '{
    "eventCode": "EVT_OMEGA_AE_CONTROL",
    "closingType": "I17G",
    "closingPeriod": "Q22021",
    "closingDate": "2021-06-30",
    "scenarioType": "CLOSING",
    "runType": "POSTING"
}');
