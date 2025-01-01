from vertexai.generative_models import GenerativeModel, GenerationConfig, Part
import json
import mimetypes

def parse_json(schema_text):
    """
    Tries to parse the output schema from the LLM model into valid JSON.
    If the schema is not valid JSON, it returns the error message.

    Args:
        schema_text (str): The schema text generated by the LLM model.

    Returns:
        dict or str: Returns the parsed JSON schema if valid, otherwise returns the error message.
    """
    try:
        schema = json.loads(schema_text)
        print(f"Schema generated successfully.")
        return schema
    except json.JSONDecodeError as e:
        # Return the error message if JSON parsing fails
        return f"JSONDecodeError"
    

def generate_schema(file_uri, existing_schema, model_name):
    """
    Generates a BigQuery-compatible schema from unstructured documents using a generative model. 
    It retains the existing schema structure, adds new inferred fields, and updates column descriptions 
    where needed.

    Args:
        file_uri (str): URI of the file containing the unstructured data.
        existing_schema (dict): Existing schema to build upon.
        model_name (str): Generative AI model for schema generation.

    Returns:
        str: Updated schema in JSON format compatible with BigQuery.
    """
        
    formatted_schema = "\n".join([str(field) for field in existing_schema]) if existing_schema else None

    prompt = f"""
    Please analyze the following documents/texts and identify the main key-value pairs that represent the core elements of the content. \n
    Your objective is to develop an updated structured data schema that organizes these elements in a clear, logical format that supports Google BigQuery for data storage and analytics. \n
    This schema should be reusable for future documents of the same type to support extracting consistent key-value pairs for analytics. \n
    If an existing schema is provided, use it as the foundation for your output. \n

    Follow the following steps (step by step) to generate the new schema: \n
    
    1. **Existing Schema:** Retain all existing column names, types, and modes as provided without modifications. Adhering strictly to this requirement is essential to avoid update Bigquery table update error . \n
    2. **New Data Elements:** Identify additional data elements only if they represent new information not already captured by an existing column. Avoid adding new columns if the data can logically map to an existing column with a similar meaning. For instance, items like "physical_requirements," "vaccination_requirements," or "desired_characteristics" could be mapped to existing "requirements" column, rather than as adding separate columns. \n
    3. **Omit Non-Analytical Fields:** Only include key-value pairs with information that can be analyzed. Exclude fields that can include lengthy text data values that are not suitable for analytics, such as job descriptions or company duties. Focus on fields that can have concise, structured data. \n
    4. Column Value Examples: For each column, you must provide at least one example value (if the value is not null) within the description. This is essential to clarify the type of data that should populate the column. Use concise keywords from the document's actual text, ensuring examples reflect realistic values for analytics later.
    5. **Updating Descriptions:** Enhance column descriptions for clarity if information in the new document suggests improvements in clarity of description, if the new document doesn't provide more clarity to description, leave the description as it is. 
    6. Do not alter existing column names or data types. \n
    7. **Array Columns for Multiple Values:** For fields that could contain multiple values (like "skills" or "benefits"), specify these columns with 'mode': 'REPEATED'. Store concise keywords instead of long descriptions, such as ["python", "data Analysis", "machine Learning"]. \n
    8. **Numerical Data and Ranges (e.g., salary):** Extract numerical values as numbers, not strings, omitting any units (e.g., "$16.00/hr" as 16.00). For ranges (e.g., "$16.00 - $18.00 per hour"), split into two columns (e.g., `hourly_wage_min` and `hourly_wage_max`) with numerical values only. Specify units like "dollars per hour" in the description. \n
    9. **Binary Columns:** For columns that can represent "yes" or "no" values, use a Boolean data type. \n
    10. **Infer Implicit Columns (e.g., Country, State):** Identify any additional information that can logically be reasoned from the document’s content, such as 'country' and 'state' based on 'location' data. For example, if a document lists "Estes Park, CO," infer columns like 'country' ("US") and 'state' ("CO"). Ensure that any such implicitly derived data is represented as separate columns in the schema. \n
    10. **Infer Implicit Columns: Go back to rule 10 **Infer Implicit Columns** and check again if you can identify additional columns that can be logically reasoned from the document.
    11. **Output Format:** Present the updated schema in a JSON format compatible with BigQuery, specifying the data type for each key. \n

    Please adhere closely to these given guidelines when constructing the schema. \n
    This schema will guide future data processing tasks to extract and insert key elements from similar documents into a BigQuery table. \n

    **Existing Schema (if available):** \n
    {formatted_schema if formatted_schema else "No existing schema"}

    **Example of Expected Output in BigQuery-compatible JSON Schema Format:** \n
    [ \n
      {{"name": "job_id", "type": "INTEGER", "mode": "NULLABLE", "description": "An identifier for job listing."}}, \n
      {{"name": "title", "type": "STRING", "mode": "NULLABLE", "description": "The title of role in job listing."}}, \n
      {{"name": "work_type", "type": "STRING", "mode": "NULLABLE", "description": "The type of work Example: full-time)."}}, \n
      {{"name": "location", "type": "STRING", "mode": "NULLABLE", "description": "The location of the role."}}, \n
      {{"name": "country", "type": "STRING", "mode": "NULLABLE", "description": "The country where role is listed, inferred from location."}}, \n
      {{"name": "skills", "type": "STRING", "mode": "REPEATED", "description": "The required skills for the role. Example: ['python', 'data Analysis', 'machine Learning']"}}, \n
      {{"name": "benefits", "type": "STRING", "mode": "REPEATED", "description": "The benefits offered for the role. Example: ['health insurance', 'retirement plan']"}}, \n
      {{"name": "hourly_wage_min", "type": "FLOAT", "mode": "NULLABLE", "description": "The minimum hourly wage for the role. The unit is dollars per hour."}}, \n
      {{"name": "hourly_wage_max", "type": "FLOAT", "mode": "NULLABLE", "description": "The maximum hourly wage for the role. The unit is dollars per hour."}} \n
    ] \n
    """

    mime_type, _ = mimetypes.guess_type(file_uri)

    file = Part.from_uri(
        uri=file_uri,
        mime_type=mime_type,
    )

    contents = [file, prompt]
    
    response_schema = {
        "type": "ARRAY",
        "items": {
            "type": "OBJECT",
            "properties": {
                "name": {"type": "STRING"},
                "type": {
                    "type": "string",
                    "enum": ["INTEGER", "STRING", "FLOAT", "BOOLEAN"],
                },
                "description": {"type": "STRING"},
                "mode": {
                    "type": "STRING",
                    "enum": ["NULLABLE", "REPEATED"],
                }
            },
            "required": ["name", "type", "description"],
        }
    }

    model = GenerativeModel(model_name)
    response = model.generate_content(
        contents,
        generation_config=GenerationConfig(
            temperature = 1,
            response_mime_type="application/json",
            response_schema = response_schema
        ),
    )
    schema = response.text
    
    schema_json = parse_json(schema)

    return schema_json


    
def transform_schema_to_response_format(schema):
    """
    Transforms a BigQuery schema into the specified response schema format.

    Args:
        schema (list): A list of dictionaries representing the BigQuery schema.

    Returns:
        dict: The transformed schema in the specified format.
    """
    properties = {}
    required_fields = []

    for field in schema:
        
        field["type"] = "NUMBER" if field["type"] == "FLOAT" else field["type"]

        # Add the field to the properties dictionary
        # Handle REPEATED mode for arrays
        if field["mode"] == "REPEATED":
            # Treat repeated fields as arrays
            properties[field["name"]] = {
                "type": "ARRAY",
                "items": {"type": field["type"]},  # The type inside the array
                "nullable": True
            }
        else:
            # Add the field to the properties dictionary for non-ARRAY types
            properties[field["name"]] = {
                "type": field["type"],
                "nullable": True
            }
            
        # Assume all fields are required, this can be modified as needed
        required_fields.append(field["name"])

    # Construct the final response schema
    response_schema = {
        "type": "ARRAY",
        "items": {
            "type": "OBJECT",
            "properties": properties,
            "required": required_fields
        }
    }
              
    print("Schema transformed to response format successfully.")

    return response_schema