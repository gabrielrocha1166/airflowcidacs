import os
import json
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

def create_base_b():
    """
    Function to create Base B with controlled noises.

    Noises added:
    - One thousand records with missing values in one of the variables.
    - Two thousand records with suppression of one random word in the variables LOGRADOURO or COMPLEMENTO.
    - Three thousand records with suppression of two random words in the variables LOGRADOURO or COMPLEMENTO.
    """

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Create BASE_B") \
        .getOrCreate()

    def remove_random_word(text):
        words = text.split()
        if words:
            word_to_remove = random.choice(words)
            words.remove(word_to_remove)
            return ' '.join(words)
        else:
            return text

    def remove_random_words(text):
        words = text.split()
        if len(words) > 1:
            words_to_remove = random.sample(words, 2)  # Choose two random words
            for word in words_to_remove:
                words.remove(word)
            return ' '.join(words)
        else:
            return text

    # Load the JSON file into a DataFrame
    df = spark.read.option("multiline", "true").json("/home/gabriel/Documents/airflowcidacs/database/data_lake/database_extracted/29274080506.json")

    # Force schema inference
    df.printSchema()

    # Counting the total number of records
    num_records = df.count()

    # Stage 1
    # Definition of the quantity of records for the first stage
    records_stage_1_quantity = 1000

    # Checks if the total number of records is less than the quantity defined for the first stage
    if num_records < records_stage_1_quantity:
        # If it's less, the dataframe for the first stage is the original dataframe
        df_stage_1 = df
    else:
        # Otherwise, a random column is added to the original dataframe
        df_with_random_column = df.withColumn("random", rand())
        # Records are randomly ordered and limited to the quantity defined for the first stage
        df_stage_1 = df_with_random_column.orderBy("random").limit(records_stage_1_quantity)
    
    # Selected records are collected
    records_stage_1 = df_stage_1.select("TIPO", "TITULO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "SETOR CENSITÁRIO").collect()
    
    # Some records have a random variable set to empty
    modified_records_stage_1 = []
    for record in records_stage_1:
        variables = record.asDict()
        empty_variables = [variable for variable, value in variables.items() if value == ""]
        
        if not empty_variables:
            variable_to_replace = random.choice(list(variables.keys()))
            variables[variable_to_replace] = ""
        
        modified_records_stage_1.append(variables)

    # Stage 2
    # Definition of the quantity of records for the second stage
    records_stage_2_quantity = 2000
    if num_records < records_stage_2_quantity:
        df_stage_2 = df
    else:
        df_with_random_column = df.withColumn("random", rand())
        df_stage_2 = df_with_random_column.orderBy("random").limit(records_stage_2_quantity)

    # Selected records are collected
    records_stage_2 = df_stage_2.select("TIPO", "TITULO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "SETOR CENSITÁRIO").collect()

    # A random word is removed from the street name and complement in each record
    modified_records_stage_2 = []
    for record in records_stage_2:
        modified_record = {
            "TIPO": record['TIPO'],
            "TITULO": record['TITULO'],
            "LOGRADOURO": remove_random_word(record['LOGRADOURO']),
            "NUMERO": record['NUMERO'],
            "COMPLEMENTO": remove_random_word(record['COMPLEMENTO']),
            "SETOR CENSITÁRIO": record['SETOR CENSITÁRIO']
        }
        modified_records_stage_2.append(modified_record)

    # Stage 3
    # Definition of the quantity of records for the third stage
    records_stage_3_quantity = 3000

    if num_records < records_stage_3_quantity:
        df_stage_3 = df
    else:
        df_with_random_column = df.withColumn("random", rand())
        df_stage_3 = df_with_random_column.orderBy("random").limit(records_stage_3_quantity)

    # Selected records are collected
    records_stage_3 = df_stage_3.select("TIPO", "TITULO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "SETOR CENSITÁRIO").collect()

    # A random word is removed from the street name and complement in each record
    modified_records_stage_3 = []
    for record in records_stage_3:
        modified_record = {
            "TIPO": record['TIPO'],
            "TITULO": record['TITULO'],
            "LOGRADOURO": remove_random_words(record['LOGRADOURO']),
            "NUMERO": record['NUMERO'],
            "COMPLEMENTO": remove_random_words(record['COMPLEMENTO']),
            "SETOR CENSITÁRIO": record['SETOR CENSITÁRIO']
        }
        modified_records_stage_3.append(modified_record)

    # Combine modified records from all stages
    final_records = modified_records_stage_1 + modified_records_stage_2 + modified_records_stage_3

    # Create the output directory if it doesn't exist
    output_folder = "/home/gabriel/Documents/airflowcidacs/database/data_warehouse/base_b"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Name of the JSON file to be saved
    json_file = os.path.join(output_folder, "BASE_B.json")

    # Save the data to a JSON file with each dictionary on a separate line
    with open(json_file, 'w', encoding='utf-8') as outfile:
        json.dump(final_records, outfile, ensure_ascii=False, indent=4)

    # Stop the Spark session
    spark.stop()

    print("Records from all stages have been combined and successfully saved to the file BASE_B.json.")

if __name__ == "__main__":
    create_base_b()