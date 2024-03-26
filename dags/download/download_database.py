import requests
import zipfile
import os

def download_database():
    """
    Downloads a database from a specific URL related to the Cadastro Nacional de Endereços para Fins Estatísticos (National Address Registry for Statistical Purposes) of the 2010 Demographic Census, extracts its contents, and saves it to a designated folder.
    """
    link = 'https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2010/BA/29274080506.zip'
    downloaded_file = 'my_file.zip'
    download_database = '/home/gabriel/Documents/airflowcidacs/database/data_lake/download_database'

    # Download the zip file
    response = requests.get(link)

    # Save the zip file content
    with open(downloaded_file, 'wb') as file:
        file.write(response.content)

    # Extract the zip file content to a specific folder
    with zipfile.ZipFile(downloaded_file, 'r') as zip_ref:
        zip_ref.extractall(download_database)

    # Remove the zip file after extraction
    os.remove(downloaded_file)

    # Get the extracted file name without extension
    file_name = os.path.splitext(os.path.basename(link))[0]

    # Path to the extracted TXT file
    txt_file = os.path.join(download_database, file_name + '.TXT')

if __name__ == "__main__":
    download_database()
    print("Download completed successfully!")
