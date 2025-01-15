
CREATE OR REPLACE PROCEDURE procedure_db.procedure_schema.SP_CDQ_EXEC_LOG (
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    QC_ID VARCHAR,
    EXEC_STATUS VARCHAR,
    ERROR_MSG VARCHAR,
    START_TS TIMESTAMP,
    END_TS TIMESTAMP,
    CYCL_TIME_ID VARCHAR
    
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

def main(session: snowpark.Session, DATABASE_NAME: str, SCHEMA_NAME: str, QC_ID: str,EXEC_STATUS :str,ERROR_MSG:str, START_TS: str, END_TS: str, CYCL_TIME_ID: str):
    try:
        # Construct the SQL query to log successful execution
        sql_query = f"""
        INSERT INTO {DATABASE_NAME}.{SCHEMA_NAME}.CDQ_EXEC_LOG (
            QC_ID, DQM_ID, DQM_NAME, DQM_DESC, DMN_CD, DMN_SUB_CD, TBL_NM, COL_NM, 
            EXCE_STATUS, ERR_MSG, START_TS, END_TS, CYCL_TIME_ID, UPDTD_BY
        ) 
        SELECT QC_ID, DQM_ID, DQM_NAME, DQM_DESC, DMN_CD, DMN_SUB_CD, TBL_NM, COL_NM, 
            '{EXEC_STATUS}', '{ERROR_MSG.replace("'","''")}', '{START_TS}', '{END_TS}', '{CYCL_TIME_ID}', CURRENT_USER()
        FROM {DATABASE_NAME}.{SCHEMA_NAME}.CDQ_CONFIG 
        WHERE QC_ID = '{QC_ID}'
        """
        
        # Execute the SQL statement
        session.sql(sql_query).collect()
        
        return 'Inserted into CDQ_EXEC_LOG successfully'
    
    except Exception as e:
        return f"Error in exe_log: {str(e)}"
$$;
