DELETE FROM CLOSING_TYPE WHERE CLOSING_TYPE_CODE IN ('I17G', 'I17L');

INSERT INTO CLOSING_TYPE (CLOSING_TYPE_CODE, CLOSING_TYPE_NAME) VALUES
('I17G', 'IFRS17 GROUP'),
('I17L', 'IFRS17 Local');