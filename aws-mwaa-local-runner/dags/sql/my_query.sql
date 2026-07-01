-- This is a sample SQL file that will be executed by the Snowflake operator
SELECT 
    CURRENT_DATE() as query_date,
    CURRENT_USER() as executing_user,
    CURRENT_ROLE() as active_role,
    'This is query number one!' as message;

-- You can safely execute multiple queries in the same file!
SELECT 'This is query number two!' as second_message;
