CREATE OR REPLACE PROCEDURE procedure_db.procedure_schema.SP_CUSTOM_DQ(json_input string)
    returns STRING
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'main'
as 

/*
        -- =========================================================================================================================
        --   Author            :     Sumiran Hirpurkar
        --   Create date       :     27/12/2024
        --   Description       :     This Proc will fetch record's from CDQ_CONFIG and execute custom queries and insert the logs 
                                     in CDQ_EXEC_LOG and CDQ_STATUS_LOG.
        --   Parameters        :     JSON which contain DB_NAME,SCHEMA_NAME,DQM_ID,DMN_CD,DMN_SUB_CD,TBL_NM,STATUS_FLAG.
        --   Return Value      :     Success/Error Details as per Success/Failure
        --   Change history    :  
    --- 02/2025
        -- =========================================================================================================================
*/

$$
import snowflake.snowpark as snowpark
from datetime import datetime
import json

def main(session: snowpark.Session, json_input: str): 
    try:
        
        # Parse the input JSON string to a dictionary
        input_data = json.loads(json_input)
        where_clauses = []  # List to store dynamic WHERE clauses
        
        # Extract required parameters from input JSON
        
        DB_NAME = input_data["DB_NAME"].strip()
        SCHEMA_NAME = input_data["SCHEMA_NAME"].strip()
        DB_ENV = input_data["DB_ENV"].strip()
        DMN_CD = input_data.get("DMN_CD")
        DMN_SUB_CD=input_data.get("DMN_SUB_CD")
        status_flag=input_data["STATUS_FLAG"]
        cycle_run_time = datetime.now()  # Capture the current time as the cycle run time
        cycle_time_id = cycle_run_time.strftime("%Y%d%m")  # Format cycle time ID as YYYYDDMM

        try:
            if not all([DB_NAME, SCHEMA_NAME, DB_ENV]) or not DMN_CD[0].strip() or not DMN_SUB_CD[0].strip():
                return "Please fill the mandatory parameters DB_NAME, SCHEMA_NAME, DB_ENV, DMN_CD, DMN_SUB_CD"
                
        except Exception as e:
            return "Please fill the mandatory parameters DB_NAME, SCHEMA_NAME, DB_ENV, DMN_CD, DMN_SUB_CD"
        
        session.sql(f"TRUNCATE TABLE DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG").collect()
        session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (1, 'Procedure started',current_timestamp())""").collect()
        start_time = datetime.now()  # Record the start time of the procedure
        session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (2, 'Start time recorded',current_timestamp())""").collect()

        
        # Build dynamic WHERE clause based on optional filters in the input JSON
        try:
            if input_data.get("DQM_ID"):
                dqm_id_tuple = f"""({', '.join(f"'{item}'" for item in input_data['DQM_ID'])})"""
                where_clauses.append(f"DQM_ID IN {dqm_id_tuple}")
            if input_data.get("DMN_CD"):
                domain_cd_tuple = f"""({', '.join(f"'{item}'" for item in input_data['DMN_CD'])})"""
                where_clauses.append(f"DMN_CD IN {domain_cd_tuple}")
            if input_data.get("DMN_SUB_CD"):
                domain_sub_cd_tuple = f"""({', '.join(f"'{item}'" for item in input_data['DMN_SUB_CD'])})"""
                where_clauses.append(f"DMN_SUB_CD IN {domain_sub_cd_tuple}")
            if input_data.get("TBL_NM"):
                table_nm = f"""({', '.join(f"'{item}'" for item in input_data['TBL_NM'])})"""
                where_clauses.append(f"TBL_NM IN {table_nm}")
            session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (3, 'WHERE clause built',current_timestamp())""").collect()
            # Combine all WHERE clauses with "AND"
            where_clause = " AND ".join(where_clauses)
            if where_clause:
                where_clause = f"{where_clause} AND"
            session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (4, 'WHERE clause adjusted',current_timestamp())""").collect()    

        except Exception as e:
            session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (5, 'Error encountered in Where clause build {str(e).replace("'","''")}',current_timestamp())""").collect()      
        
        
        result_set_1=""
        result_set3=""
        try:
            if status_flag.strip()=='':
                # Step 1: Fetch active custom data quality (CDQ) configurations
                result_set_1=session.sql(f"""
                            SELECT  * 
                            FROM DQ_{DB_ENV}.{SCHEMA_NAME}.CDQ_CONFIG 
                            WHERE {where_clause} IS_ACTIVE = '1' 
                        """).collect()
            
                session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (6, 'Fetched active CDQ  all configurations',current_timestamp())""").collect()    
        except Exception as e:
            session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (7, 'Error encountered while fetching CDQ  all configurations {str(e).replace("'","''")}',current_timestamp())""").collect()    

            
        status_flag=status_flag.upper()   
        try:
            if status_flag=='F':
                result_set_1=session.sql(f"""
                    SELECT QC_ID || DQM_ID || DMN_CD || DMN_SUB_CD as compo_key, * 
                    FROM DQ_{DB_ENV}.{SCHEMA_NAME}.CDQ_CONFIG 
                    WHERE {where_clause} IS_ACTIVE = '1' 
                    AND compo_key  IN (
                        SELECT QC_ID || DQM_ID || DMN_CD || DMN_SUB_CD
                        FROM DQ_{DB_ENV}.{SCHEMA_NAME}.CDQ_EXEC_LOG
                        WHERE CYCL_TIME_ID = '{cycle_time_id}' and EXCE_STATUS='F'
                    )
                """).collect()
                session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (8, 'Fetched records with status_flag is F',current_timestamp())""").collect()
        except Exception as e:
            session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (9, 'Error encountered while fetching  records with status_flag is F {str(e).replace("'","''")}',current_timestamp())""").collect() 
        try:
            if status_flag=='R':
                result_set_1 = session.sql(f"""
                SELECT QC_ID || DQM_ID || DMN_CD || DMN_SUB_CD as compo_key, * 
                FROM DQ_{DB_ENV}.{SCHEMA_NAME}.CDQ_CONFIG 
                WHERE {where_clause} IS_ACTIVE = '1' 
                AND compo_key NOT IN (
                    SELECT QC_ID || DQM_ID || DMN_CD || DMN_SUB_CD
                    FROM DQ_{DB_ENV}.{SCHEMA_NAME}.CDQ_EXEC_LOG
                    WHERE CYCL_TIME_ID = '{cycle_time_id}'
                )
            """).collect()
                session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (10, 'Fetched records with status_flag R',current_timestamp())""").collect()
        except Exception as e:
            session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (11, 'Error encountered while fetching records with status_flag R {str(e).replace("'","''")}',current_timestamp())""").collect()    
        
        
        # Iterate through each configuration record fetched
        for row in result_set_1:
            session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (12, 'Processing a row for QC_ID: {row["QC_ID"]}',current_timestamp())""").collect()
            sql_query = row["SQL_QUERY"]  # Fetch the SQL query
            threshold_operator = row["THRESHOLD_OPERATOR"]  # Fetch the threshold operator
            threshold_value = row["THRESHOLD_VALUE"]  # Fetch the threshold value

            # Replace placeholder "DATABASE_NAME" with the actual database name
            sql_query_v1 = sql_query.replace("DATABASE_NAME", DB_NAME)

            # Record the start time for SQL execution
            start_time = datetime.now()
            try:
                
                # Execute the SQL query and fetch results
                
                result_set_2 = session.sql(sql_query_v1).collect()
                
            except Exception as query_error:
                
                # Log the error if SQL query execution fails
                error_message = str(query_error).replace("'", "")
                end_time = datetime.now()
                query_result=session.sql(f"""CALL SP_CDQ_EXEC_LOG('DQ_{DB_ENV}','{SCHEMA_NAME}','{row["QC_ID"]}','F','{error_message}','{start_time}','{end_time}','{cycle_time_id}')""").collect()
                
                response_message = query_result[0][0].replace("'","")
                # Check if "successfully" is in the response, otherwise raise an exception
                if "successfully" not in response_message.lower():
                    session.sql(f"""
                    INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES 
                    (13, 'Error in SP_CDQ_EXEC_LOG : {response_message}',current_timestamp())
                """).collect()
                    
                session.sql(f"""
                    INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES 
                    (14, 'SQL query failed: {error_message}',current_timestamp())
                """).collect()
                # Skip to the next record on failure
                continue    

            # Fetch the first result or set default output to 0
            output = result_set_2[0][0] if result_set_2 else 0
            
            # Record the end time for SQL execution
            end_time = datetime.now()

            # Log successful execution in CDQ_EXEC_LOG
            try:
                query_result=session.sql(f"""CALL SP_CDQ_EXEC_LOG('DQ_{DB_ENV}','{SCHEMA_NAME}','{row["QC_ID"]}','P','','{start_time}','{end_time}','{cycle_time_id}')""").collect()
                response_message = query_result[0][0].replace("'","")
                # Check if "successfully" is in the response, otherwise raise an exception
                if "successfully" not in response_message.lower():
                    raise Exception(f"{response_message}")
                
            except Exception as e:
                session.sql(f"""
            INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES 
            (15, 'SP_CDQ_EXEC_LOG Procedure failed for {row["QC_ID"]}: {str(e).replace("'", "''")}',current_timestamp())
        """).collect()
                
            
            # Log execution details in CDQ_STATUS_LOG
            try:
                query_result=session.sql(f"""CALL SP_CDQ_STATUS_LOG('DQ_{DB_ENV}','{SCHEMA_NAME}','{row["QC_ID"]}','{sql_query_v1.replace("'","''")}','{start_time}','{end_time}','{cycle_time_id}','{output}','{threshold_operator}','{threshold_value}')""").collect()
                response_message = query_result[0][0].replace("'","")
                # Check if "successfully" is in the response, otherwise raise an exception
                if "successfully" not in response_message.lower():
                    raise Exception(f"{response_message}")

            except Exception as e:
                session.sql(f"""
            INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES 
            (16, 'SP_CDQ_STATUS_LOG Procedure failed for {row["QC_ID"]}: {str(e).replace("'", "''")}',current_timestamp())
        """).collect()
        # Return success message after processing all records
        session.sql(f"""INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES (17, 'Procedure execution completed successfully',current_timestamp())""").collect()
        return "SP_CUSTOM_DQ executed successfully"

    except Exception as e:
        # Return error message if any exception occurs
        session.sql(f"""
            INSERT INTO DQ_{DB_ENV}.{SCHEMA_NAME}.CUSTOM_DQ_PROCESS_LOG VALUES 
            (18, 'SP_CUSTOM_DQ Procedure failed: {str(e).replace("'", "''")}',current_timestamp())
        """).collect()
        return f"Error: {str(e)}"
$$;
