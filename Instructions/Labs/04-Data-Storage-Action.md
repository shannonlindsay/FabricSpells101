---
lab:
    title: 'Lab 3'
    module: 'Data Storage and Action'
---

![Illustration of a bar chart made of potion bottles with a witch flying above.](https://github.com/shannonlindsay/WitchesGuide/assets/77289548/f9560ec5-edb1-4b9d-aa38-e57954e1d3dd)

# Lab 3 - Data Storage and Action in Fabric

In this lab you'll create both a SQL database and a user data function.

- [Create a SQL database](https://scribehow.com/viewer/3a_Create_a_SQL_database_in_Fabric__bYEC2JgDTsy0iDeZAckalw?referrer=documents)
- [Create and test a user data function](https://scribehow.com/viewer/3b_Create_and_Test_a_Fabric_User_Data_Functions__8XIFX8FIRgaSnqEGdL5QCQ?referrer=documents)
    - Note: Copy and paste the code below for your **user data function**

```
import fabric.functions as fn
import logging

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="fabconsessions")
@udf.function()
def save_to_agenda(sqlDB: fn.FabricSqlConnection, sessionid: int, Attendee: str, Status: str) -> str:

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
  
    logging.info("Adding attendee ... ")
    # Create the table if it doesn't exist
    create_table_query = '''
        IF OBJECT_ID(N'dbo.attendees', N'U') IS NULL
        CREATE TABLE dbo.attendees (
            [session_id] bigint NOT NULL,
            [Attendee] NVARCHAR(250) NOT NULL,
            [Status] NVARCHAR(50) NOT NULL
            );
    '''
    cursor.execute(create_table_query)
 
    # Insert data into the table
    # insert_query = "INSERT INTO dbo.attendees (session_id, Attendee, Status) VALUES (?, ?, ?);"
    merge_query = """
        MERGE dbo.attendees AS target
        USING (SELECT ? AS session_id, ? AS Attendee) AS src
        ON target.session_id = src.session_id AND target.Attendee = src.Attendee
        WHEN MATCHED THEN
            UPDATE SET Status = ?
        WHEN NOT MATCHED THEN
            INSERT (session_id, Attendee, Status)
            VALUES (?, ?, ?);
    """

     # Provide a value for each question mark above, in order:
    merge_params = (
        sessionid,              # SELECT ? AS session_id
        Attendee,               # SELECT ? AS Attendee
        Status,                 # UPDATE SET [Status] = ?
        sessionid,              # INSERT VALUES (?, ...
        Attendee,               # ...
        Status                  # ...?)
    )   
    cursor.execute(merge_query, merge_params)
    logging.info("Attendee was added/modified")

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()               
    return "Attendee table was created (if necessary) and data was added to this table"

@udf.connection(argName="sqlDB",alias="fabconsessions")
@udf.function()
def delete_from_agenda(sqlDB: fn.FabricSqlConnection, sessionid: int, Attendee: str) -> str:

    data = (sessionid, Attendee)

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
 
    logging.info("Removing attendee")
    # Insert data into the table
    insert_query = "DELETE FROM dbo.attendees WHERE session_id = ? and Attendee = ?;"
    cursor.execute(insert_query, data)
    logging.info("Attendee was remeoved")

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()               
    return "Attendee was removed"

```
