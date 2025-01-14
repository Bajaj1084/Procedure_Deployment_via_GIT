       
CREATE OR REPLACE PROCEDURE SP_CDQ_STATUS_LOG (
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    QC_ID VARCHAR,
    SQL_QUERY VARCHAR,
    START_TS TIMESTAMP,
    END_TS TIMESTAMP,
    CYCL_TIME_ID VARCHAR,
    OUTPUT VARCHAR,
    THRESHOLD_OPERATOR VARCHAR,
    THRESHOLD_VALUE VARCHAR
    
    
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from datetime import datetime

def main(session: snowpark.Session, DATABASE_NAME: str, SCHEMA_NAME: str, QC_ID: str,SQL_QUERY: str,  
         START_TS: str, END_TS: str, CYCL_TIME_ID: str,OUTPUT:str,THRESHOLD_OPERATOR:str,THRESHOLD_VALUE:str):
    try:
        # Construct the SQL query to log the status
        
        
        sql_query = f"""
        INSERT INTO {DATABASE_NAME}.{SCHEMA_NAME}.CDQ_STATUS_LOG (
            QC_ID, DQM_ID, DQM_NAME, DQM_DESC, DMN_CD, DMN_SUB_CD, TBL_NM, COL_NM, 
            DQ_STATUS, DQ_QUERY, IS_CRITICAL, START_TS, END_TS, CYCL_TIME_ID, UPDTD_BY
        ) 
        SELECT QC_ID, DQM_ID, DQM_NAME, DQM_DESC, DMN_CD, DMN_SUB_CD, TBL_NM, COL_NM, CASE WHEN  {OUTPUT} {THRESHOLD_OPERATOR} {THRESHOLD_VALUE} THEN 'P' ELSE 'F' END AS DQ_STATUS,
            '{SQL_QUERY.replace("'","''")}', IS_CRITICAL, '{START_TS}', '{END_TS}', '{CYCL_TIME_ID}', CURRENT_USER()
        FROM {DATABASE_NAME}.{SCHEMA_NAME}.CDQ_CONFIG 
        WHERE QC_ID = '{QC_ID}'
        """
    
        # Execute the SQL statement
        session.sql(sql_query).collect()
    
        return 'Inserted into CDQ_STATUS_LOG successfully'
    
    except Exception as e:
        return f"Error in SP_Status_Log: {str(e)}"
$$;
