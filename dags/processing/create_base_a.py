import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import rand

def create_base_a():
    """
    Function to create BASE_A, which consists of cropping a specific number of random address records from a JSON database,
    selecting the variables: TIPO, TITULO, LOGRADOURO, NUMERO, COMPLEMENTO, and SETOR CENSITÁRIO.

    This function loads a JSON file into a Spark DataFrame, counts the number of records, and randomly selects the desired number of records,
    as specified by the 'desired_records' variable. The selected records are then transformed into a dictionary format and saved to a JSON file.
    """

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Create BASE_A") \
        .getOrCreate()

    # Load the JSON file into a DataFrame
    df = spark.read.option("multiline", "true").json("/home/gabriel/Documents/airflowcidacs/database/data_lake/database_extracted/29274080506.json")

    # Force schema inference
    df.printSchema()

    # Count the number of records in the DataFrame
    num_records = df.count()

    # Define the desired number of records to be selected
    desired_records = 100000

    # If the number of records is less than or equal to the desired quantity,
    # we will use all records, otherwise, we will select randomly
    if num_records <= desired_records:
        selected_df = df
    else:
        # Add a random column for sorting
        df_with_random_column = df.withColumn("random", rand())
        
        # Select randomly the desired quantity of records
        selected_df = df_with_random_column.orderBy("random").limit(desired_records)

    # Select the necessary variables and collect the records into a list
    records = selected_df.select("TIPO", "TITULO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "SETOR CENSITÁRIO").collect()

    # Transform the records into a list of dictionaries
    records_dict = [r.asDict() for r in records]

    # Create the output directory if it doesn't exist
    output_folder = "/home/gabriel/Documents/airflowcidacs/database/data_warehouse/base_a"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Name of the JSON file to be saved
    json_file = os.path.join(output_folder, "BASE_A.json")

    # Save the data to a JSON file
    with open(json_file, 'w', encoding='utf-8') as outfile:
        json.dump(records_dict, outfile, ensure_ascii=False, indent=4)

    # Stop the Spark session
    spark.stop()

    print("One hundred thousand random records were successfully extracted and saved.")

if __name__ == "__main__":
    create_base_a()