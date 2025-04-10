from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from .config import logger, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE

def load_private_key():
    """
    Loads and converts a private key from a PEM file to DER format for Snowflake authentication.

    This function reads a private key from a file named 'private_key.p8', decrypts it using a
    hardcoded password, and converts it to DER format, which is required for Snowflake's private
    key authentication.

    Returns:
        bytes: The private key in DER format.

    Raises:
        FileNotFoundError: If the 'private_key.p8' file is not found.
        ValueError: If the private key cannot be loaded or decrypted.
    """
    # Open and read the private key file in binary mode
    with open("private_key.p8", "rb") as key_file:
        # Load the PEM-encoded private key, decrypting it with the specified password
        private_key_obj = serialization.load_pem_private_key(
            key_file.read(),
            password=b'zrgtg3!',  # Hardcoded password for decryption (consider securing this in production)
            backend=default_backend()
        )
    # Convert the private key to DER format (binary) without encryption
    return private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def create_session():
    """
    Creates a Snowflake session using Snowpark with private key authentication.

    This function loads the private key using load_private_key(), constructs connection parameters
    using environment variables from the config module, and establishes a Snowpark session for
    interacting with Snowflake.

    Returns:
        Session: A Snowpark Session object connected to Snowflake.

    Raises:
        Exception: If the session cannot be created due to invalid credentials or connectivity issues.
    """
    # Load the private key in DER format for authentication
    private_key_der = load_private_key()
    # Define connection parameters using environment variables and the private key
    connection_params = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "private_key": private_key_der, 
        "database": SNOWFLAKE_DATABASE, 
        "schema": SNOWFLAKE_SCHEMA, 
        "warehouse": SNOWFLAKE_WAREHOUSE 
    }
    # Create and return a Snowpark session with the specified parameters
    return Session.builder.configs(connection_params).create()

def setup_snowflake_tables(session):
    """
    Sets up the required Snowflake tables for storing scraped data.

    This function ensures the session is using the correct database and schema, then creates or
    replaces two tables: ONLINE_RESOURCES (for navigation data) and ONLINE_RESOURCES_TXT (for text
    content). It logs the success or failure of table creation.

    Args:
        session (Session): A Snowpark Session object connected to Snowflake.

    Raises:
        Exception: If SQL commands fail due to permissions, connectivity, or syntax errors.
    """
    # Set the database and schema context for the session
    session.sql("USE DATABASE SNOWFLAKE_DOCUMENTATION").collect()
    session.sql("USE SCHEMA STAGING").collect()
    logger.info("Set context to SNOWFLAKE_DOCUMENTATION.STAGING")

    # Create or replace the ONLINE_RESOURCES table for navigation data
    session.sql("""
        CREATE OR REPLACE TABLE ONLINE_RESOURCES (
            ID VARCHAR,          -- Unique identifier for the navigation entry
            URL VARCHAR,         -- Fully-qualified URL of the page
            LABEL VARCHAR,       -- Display label for the navigation entry
            TYPE VARCHAR,        -- Type of navigation entry (e.g., page, section)
            DEPTH INTEGER,       -- Depth in the navigation hierarchy
            PARENT_URL VARCHAR,  -- URL of the parent navigation entry
            SECTION VARCHAR      -- Section name (e.g., 'guides', 'developer')
        )
    """).collect()
    # Verify that the table was created successfully
    if session.sql("SHOW TABLES LIKE 'ONLINE_RESOURCES'").collect():
        logger.info("Table ONLINE_RESOURCES created or replaced successfully.")
    else:
        logger.error("Table ONLINE_RESOURCES was not created!")

    # Create or replace the ONLINE_RESOURCES_TXT table for text content
    session.sql("""
        CREATE OR REPLACE TABLE ONLINE_RESOURCES_TXT (
            URL VARCHAR,         -- Fully-qualified URL of the page
            CONTENT VARCHAR,     -- Scraped text content of the page
            SECTION VARCHAR      -- Section name (e.g., 'guides', 'developer')
        )
    """).collect()
    # Verify that the table was created successfully
    if session.sql("SHOW TABLES LIKE 'ONLINE_RESOURCES_TXT'").collect():
        logger.info("Table ONLINE_RESOURCES_TXT created or replaced successfully.")
    else:
        logger.error("Table ONLINE_RESOURCES_TXT was not created!")

def upload_navigation_data(session, tree_data, section_name):
    """
    Uploads navigation data to the ONLINE_RESOURCES table in Snowflake.

    This function takes a list of navigation entries (tree_data), adds the section name to each
    entry, converts the data into a Snowpark DataFrame, and appends it to the ONLINE_RESOURCES table.

    Args:
        session (Session): A Snowpark Session object connected to Snowflake.
        tree_data (list): List of dictionaries containing navigation data.
        section_name (str): The name of the section (e.g., 'guides', 'developer').

    Raises:
        Exception: If the upload fails due to schema mismatches, permissions, or connectivity issues.
    """
    # Only proceed if there is data to upload
    if tree_data:
        # Add the section name to each navigation entry
        for row in tree_data:
            row['section'] = section_name
        # Convert the list of dictionaries to a Snowpark DataFrame
        df = session.create_dataframe(tree_data)
        # Append the DataFrame to the ONLINE_RESOURCES table
        df.write.mode("append").save_as_table("ONLINE_RESOURCES")
        logger.info(f"Uploaded {len(tree_data)} rows to ONLINE_RESOURCES (section={section_name}).")

def upload_text_data(session, txt_data, section_name):
    """
    Uploads text content data to the ONLINE_RESOURCES_TXT table in Snowflake.

    This function takes a list of text content entries (txt_data), adds the section name to each
    entry, converts the data into a Snowpark DataFrame, and appends it to the ONLINE_RESOURCES_TXT table.

    Args:
        session (Session): A Snowpark Session object connected to Snowflake.
        txt_data (list): List of dictionaries containing text content data.
        section_name (str): The name of the section (e.g., 'guides', 'developer').

    Raises:
        Exception: If the upload fails due to schema mismatches, permissions, or connectivity issues.
    """
    # Only proceed if there is data to upload
    if txt_data:
        # Add the section name to each text content entry
        for row in txt_data:
            row['section'] = section_name
        # Convert the list of dictionaries to a Snowpark DataFrame
        df = session.create_dataframe(txt_data)
        # Append the DataFrame to the ONLINE_RESOURCES_TXT table
        df.write.mode("append").save_as_table("ONLINE_RESOURCES_TXT")
        logger.info(f"Uploaded {len(txt_data)} rows to ONLINE_RESOURCES_TXT (section={section_name}).")