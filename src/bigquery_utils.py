from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest
import time

def get_bigquery_table_schema(project_id, dataset_id, table_id):
    """
    Retrieves the schema of a BigQuery table in JSON format.

    Args:
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.

    Returns:
        list: A list of dictionaries representing the schema fields with 'name', 'type', 
              and optionally 'mode' and 'description'.
        None: If the table is not found.
    """
        
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Construct the full table ID (project.dataset.table)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        # Get the table object
        table = client.get_table(table_ref)
        
        # Get the schema in JSON format
        schema = [
            {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description if field.description else ""
            }
            for field in table.schema
        ]


        print(f"Table {table_id} exists. Returning schema.")
        return schema
    except NotFound:
        # If the table does not exist, return None
        print(f"Table {table_id} not found. No schema returned.")
        return None
    
    
def create_new_table(client, table_ref, schema):
    """
    Creates a new BigQuery table with the specified schema.

    Args:
        client (bigquery.Client): BigQuery client instance.
        table_ref (str): Reference to the BigQuery table (project.dataset.table).
        schema (list): Schema definition for the new table.

    Returns:
        str: Success message if the table is created successfully.
        str: Error message if table creation fails.
    """
    
    try:
        table = bigquery.Table(table_ref, schema)
        client.create_table(table)
        print(f"Table {table.table_id} created successfully with the new schema.")
        return "Success"
    except BadRequest as e:
        print(f"Failed to create new table {table_ref}: {e}")
        return "Error"

    
def update_existing_table(client, table, schema):
    """
    Updates the schema of an existing BigQuery table if there are changes.

    Args:
        client (bigquery.Client): BigQuery client instance.
        table (bigquery.Table): The BigQuery table object to be updated.
        schema (list): The new schema to update the table with.

    Returns:
        str: Success message if the schema is updated or if no changes were necessary.
        str: Error message if the schema update fails.
    """
    
    try:
        # Compare the existing schema with the new schema
        if table.schema != schema:
            # If schemas don't match, update the table schema
            table.schema = schema
            client.update_table(table, ["schema"])
            print(f"Schema of table {table.table_id} updated successfully.")
            return "Success"
        else:
            print(f"Schema of table {table.table_id} is already up to date. No changes made.")
            return "Success"
    except BadRequest as e:
        print(f"Failed to update schema of table {table.table_id}: {e}")
        return "Error"

    
def create_or_update_table(dataset_id, table_id, new_schema, project_id):
    """
    Updates an existing BigQuery table schema or creates a new table if it doesn't exist.

    Args:
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        new_schema (list): New schema definition in JSON format.
        project_id (str): Google Cloud project ID.

    Returns:
        str: Message indicating whether the table schema was updated or a new table was created.
    """
    
    client = bigquery.Client(project=project_id)
     
    # Convert the JSON schema to BigQuery schema format
    bigquery_schema = [
        bigquery.SchemaField(
            field["name"], 
            field["type"], 
            mode=field.get("mode", "NULLABLE"),  # Include mode if it exists, default to "NULLABLE"
            description=field.get("description", "")
        )
        for field in new_schema
    ]
    
    dataset_ref = f"{project_id}.{dataset_id}"

    # Check if dataset exists or create it
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created successfully.")

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        # Try to get the existing table
        table = client.get_table(table_ref)
        return update_existing_table(client, table, bigquery_schema)
    except NotFound:
        # If table doesn't exist, create a new one
        return create_new_table(client, table_ref, bigquery_schema)

    
def insert_json_to_bigquery(project_id, dataset_id, table_id, json_data, max_retries=10, delay_between_retries=10):
    """
    Inserts key-value pairs (in JSON format) into an existing BigQuery table.

    Args:
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        json_data (dict): Key-value pairs in JSON format to insert into the table.
        max_retries (int): Maximum number of retries in case of errors.
        delay_between_retries (int): Delay between retries in seconds.

    Returns:
        str: Status message indicating success or failure.
    """
    
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Construct the full table ID (project.dataset.table)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    for attempt in range(max_retries):
        try:
            # Make an API request to insert rows
            errors = client.insert_rows_json(table_ref, json_data)

            if errors == []:
                print(f"Row inserted successfully in {table_id}.") 
                return "Success"
            else:
                print(f"Attempt {attempt + 1} failed: Encountered errors while inserting row in {table_id}: {errors}")

        except NotFound as e:
            # Handle NotFound exception (e.g., if the table does not exist)
            print(f"Attempt {attempt + 1} failed: Table {table_id} not found: {e}")

        # If this is not the last attempt, wait before retrying
        if attempt < max_retries - 1:
            time.sleep(delay_between_retries)
    
    print(f"Max retries of {max_retries} reached. Failed to insert row in {table_id}.")
    return "Error"


def get_random_rows_from_bigquery(project_id, dataset_id, table_id, num_rows):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)

    try:
        # Define the SQL query to fetch a random set of rows
        query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        ORDER BY RAND()
        LIMIT {num_rows}
        """

        # Run the query
        query_job = client.query(query)

        # Get the results
        results = query_job.result()

        # Convert to list of dictionaries (optional)
        rows = [dict(row.items()) for row in results]
        
        print(f"{num_rows} examples from table {table_id} retrieved successfully.")

        return rows
    except Exception as e:
        print(f"Failed to retrieve {num_rows} examples from {table_id}: {e}")
        return []