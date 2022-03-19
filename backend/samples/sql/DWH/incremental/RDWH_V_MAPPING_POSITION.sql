DROP VIEW V_MAPPING_POSITION;

CREATE OR REPLACE VIEW V_MAPPING_POSITION AS 
(
	SELECT 
			 PM.MAPPING_POSITION_ID AS MAP_POSITION_ID               
			,P1.CODE            	AS MAP_POSITION_CODE         
			,P1.NAME            	AS MAP_POSITION_NAME        
			,P1.IS_IPDS         	AS IS_IPDS_MAP                    
			,P1.IS_CSM          	AS IS_CSM_MAP
			,P1.IS_COMPOSITE		AS IS_COMPOSITE_MAP
			,P1.IS_DELTA        	AS IS_DELTA_MAP
			,PM.MATH_OPERATOR       AS MATH_OPERATOR      
			,PM.POSITION_ID     	AS SOURCE_POSITION_ID      
			,P2.CODE            	AS SOURCE_POSITION_CODE  
			,P2.NAME            	AS SOURCE_POSITION_NAME
			,P2.IS_IPDS           AS IS_IPDS                                            
			,P2.IS_CSM           AS IS_CSM
			,P2.IS_COMPOSITE  AS IS_COMPOSITE
			,P2.IS_DELTA            AS IS_DELTA
			,P2.IS_CASH_FLOW  AS IS_CASH_FLOW
			,P2.VALID_FROM    AS VALID_FROM                     
			,P2.VALID_TO      AS VALID_TO                         
	FROM
		 BI_<env>.POSITION_MAPPING AS PM
		 INNER JOIN BI_<env>.POSITION AS P1	
		 ON PM.MAPPING_POSITION_ID = P1.ID 		 
		 INNER JOIN BI_<env>.POSITION AS P2
		 ON PM.POSITION_ID = P2.ID
		AND P1.IS_LOCAL IS FALSE
UNION
	SELECT 
			 P1.ID              AS MAP_POSITION_ID               
			,P1.CODE            AS MAP_POSITION_CODE         
			,P1.NAME            AS MAP_POSITION_NAME        
			,P1.IS_IPDS         AS IS_IPDS_MAP                    
			,P1.IS_CSM          AS IS_CSM_MAP
			,P1.IS_COMPOSITE	AS IS_COMPOSITE_MAP
			,P1.IS_DELTA        AS IS_DELTA_MAP
		  	,NULL               AS MATH_OPERATOR                
			,P2.ID              AS SOURCE_POSITION_ID      
			,P2.CODE            AS SOURCE_POSITION_CODE  
			,P2.NAME            AS SOURCE_POSITION_NAME
			,P2.IS_IPDS           AS IS_IPDS
			,P2.IS_CSM           AS IS_CSM
			,P2.IS_COMPOSITE  AS IS_COMPOSITE
			,P2.IS_DELTA  AS IS_DELTA
			,P2.IS_CASH_FLOW  AS IS_CASH_FLOW
			,P2.VALID_FROM    AS VALID_FROM                     
			,P2.VALID_TO      AS VALID_TO                         
	FROM
		 BI_<env>.POSITION AS P1	
		 INNER JOIN BI_<env>.POSITION AS P2
		 ON P1.ID = P2.ID
			AND P1.VALID_FROM = P2.VALID_FROM
			AND P1.VALID_TO = P2.VALID_TO
			AND P1.POSITION_TYPE_ID = 1
			AND P2.POSITION_TYPE_ID = 1
);

