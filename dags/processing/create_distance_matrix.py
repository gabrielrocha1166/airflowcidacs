import os
import json
import textdistance


def create_distance_matrix_():
    """
    Function to create a distance matrix between records from bases A and B.

    Utilizes the Jaro-Winkler distance measure to calculate the distance between records
    based on the 'LOGRADOURO' and 'COMPLEMENTO' variables.

    The input for this function is two sets of records, base_A and base_B, where each record
    is a dictionary with keys corresponding to the variable names ('LOGRADOURO' and 'COMPLEMENTO').

    Returns a JSON file containing a list of dictionaries, each representing the distance between
    a pair of records from bases A and B. Each dictionary contains the keys:
    - 'LOGRADOURO_A': Value of the 'LOGRADOURO' variable for the record in base A.
    - 'LOGRADOURO_B': Value of the 'LOGRADOURO' variable for the record in base B.
    - 'dist_LOGRADOURO': Jaro-Winkler distance between the 'LOGRADOURO' values of the records.
    - 'COMPLEMENTO_A': Value of the 'COMPLEMENTO' variable for the record in base A.
    - 'COMPLEMENTO_B': Value of the 'COMPLEMENTO' variable for the record in base B.
    - 'dist_COMPLEMENTO': Jaro-Winkler distance between the 'COMPLEMENTO' values of the records.
    """

    # Loading bases A and B
    with open('/home/gabriel/Documents/airflowcidacs/database/data_warehouse/base_a/BASE_A.json', 'r') as f:
        base_A = json.load(f)

    with open('/home/gabriel/Documents/airflowcidacs/database/data_warehouse/base_b/BASE_B.json', 'r') as f:
        base_B = json.load(f)

    # List to store the results
    results = []

    # Calculating Jaro-Winkler distance
    for record_A in base_A:
        for record_B in base_B:
            dist_ADDRESS = textdistance.jaro_winkler(record_A["LOGRADOURO"], record_B["LOGRADOURO"])
            dist_COMPLEMENT = textdistance.jaro_winkler(record_A["COMPLEMENTO"], record_B["COMPLEMENTO"])
            
            # Creating a dictionary with the results
            result = {
                'LOGRADOURO_A': record_A["LOGRADOURO"],
                'LOGRADOURO_B': record_B["LOGRADOURO"],
                'dist_LOGRADOURO': dist_ADDRESS,
                'COMPLEMENTO_A': record_A["COMPLEMENTO"],
                'COMPLEMENTO_B': record_B["COMPLEMENTO"],
                'dist_COMPLEMENTO': dist_COMPLEMENT
            }
            
            # Adding the dictionary to the results list
            results.append(result)

    # Create the output directory if it doesn't exist
    output_folder = "/home/gabriel/Documents/airflowcidacs/database/data_warehouse/distance_matrix"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Saving the results to a JSON file
    with open('/home/gabriel/Documents/airflowcidacs/database/data_warehouse/distance_matrix/distance_matrix.json', 'w') as json_file:
        json.dump(results, json_file, indent=4)

if __name__ == "__main__":
    create_distance_matrix_()
    print("File 'distance_matrix.json' created and saved successfully!")