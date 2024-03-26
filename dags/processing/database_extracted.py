import os
import json

def database_extracted():
    """
    The function performs the following steps:
    - Reads the TXT file line by line.
    - Extracts information from each line according to a predefined layout.
    - Creates a dictionary with the extracted information.
    - Checks and organizes the address and complement parts.
    - Adds the address and complement to the dictionary.
    - Adds the dictionary to the JSON data list.

    After processing all lines of the TXT file, the data is saved to a JSON file in the specified
    directory.
    """

    download_database = '/home/gabriel/Documents/airflowcidacs/database/data_lake/download_database'
    output_folder = '/home/gabriel/Documents/airflowcidacs/database/data_lake/database_extracted'
    link = 'https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2010/BA/29274080506.zip'
    
    # Get the extracted file name without extension
    file_name = os.path.splitext(os.path.basename(link))[0]

    # Path to the extracted TXT file
    txt_file = os.path.join(download_database, file_name + '.TXT')

    # List to store data in JSON format
    json_data = []

    # Open the TXT file and read line by line
    with open(txt_file, 'r', encoding='latin-1') as file:
        for line in file:
            # Extract information from each line
            state = line[0:2].strip()
            municipality = line[2:7].strip()
            census_sector = line[7:11].strip()
            face = line[11:16].strip()
            type_ = line[16:36].strip()
            title = line[36:66].strip()
            name = line[66:127].strip()
            number = line[127:134].strip()
            modifier = line[134:141].strip()
            element_1 = line[141:161].strip()
            value_1 = line[161:171].strip()
            element_2 = line[171:191].strip()
            value_2 = line[191:201].strip()
            element_3 = line[201:221].strip()
            value_3 = line[221:231].strip()
            element_4 = line[231:251].strip()
            value_4 = line[251:271].strip()
            element_5 = line[271:291].strip()
            value_5 = line[291:301].strip()
            element_6 = line[301:321].strip()
            value_6 = line[321:331].strip()
            element_7 = line[331:351].strip()
            locality = line[351:471].strip()
            address_type = line[471:473].strip()
            geocoding_level = line[513:514].strip()

            # Check if space 6 of the "CENSUS SECTOR" field is a space and replace it with '0'
            census_sector_checked = []
            for digit in census_sector:
                if digit == ' ':
                    digit = '0'  # Correction here
                census_sector_checked.append(digit)
            census_sector = ''.join(census_sector_checked)

            # Create a dictionary with the extracted information
            data = {
                'UF': state,
                'MUNICIPIO': municipality,
                'SETOR CENSITÁRIO': census_sector,
                'FACE': face,
                'TIPO': type_,
                'TITULO': title,
                'NOME': name,
                'NUMERO': number,
                'MODIFICADOR': modifier,
                'LOCALIDADE': locality,
                'ESPECIE DE ENDEREÇO': address_type,
                'NIVEL DE GEOCODIFICACAO': geocoding_level,
            }

            # Check if each of the variables for the address is not empty and add only non-empty ones
            address_parts = []
            if type_:
                address_parts.append(type_)
            if title:
                address_parts.append(title)
            if name:
                address_parts.append(name)

            # Add the address to the dictionary
            data['LOGRADOURO'] = ' '.join(address_parts)

            # Check if each of the variables for the complement is not empty and add only non-empty ones
            complement_parts = []
            if element_1:
                complement_parts.append(element_1)
            if value_1:
                complement_parts.append(value_1)
            if element_2:
                complement_parts.append(element_2)
            if value_2:
                complement_parts.append(value_2)
            if element_3:
                complement_parts.append(element_3)
            if value_3:
                complement_parts.append(value_3)
            if element_4:
                complement_parts.append(element_4)
            if value_4:
                complement_parts.append(value_4)
            if element_5:
                complement_parts.append(element_5)
            if value_5:
                complement_parts.append(value_5)
            if element_6:
                complement_parts.append(element_6)
            if value_6:
                complement_parts.append(value_6)
            if element_7:
                complement_parts.append(element_7)

            # Add the complement to the dictionary if there are non-empty parts
            data['COMPLEMENTO'] = ' '.join(complement_parts)

            # Add the dictionary to the JSON data list
            json_data.append(data)

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Name of the JSON file to be saved
    json_file = os.path.join(output_folder, file_name + '.json')

    # Save the data to a JSON file inside the output folder
    with open(json_file, 'w', encoding='utf-8') as outfile:
        json.dump(json_data, outfile, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    database_extracted()
    print("JSON file generated successfully!")