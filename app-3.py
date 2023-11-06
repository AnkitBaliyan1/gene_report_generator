import time
import pandas as pd
import streamlit as st
import os

glb_root = ("04_resources/")
glb_01_input = (glb_root + r"01_input")
glb_02_master = (glb_root + r"02_master")
glb_03_output = (glb_root + r"03_output")
glb_04_temp_input = (glb_root + r"04_temp_input")
glb_05_mapping_files = (glb_root + r"05_mapping_files" + r"\P01_caffeine")
glb_masterfile =  glb_02_master + "\\Caffeine_Master_New.xlsx"
##### For report generation
glb_template_root = (glb_root + r"\06_templates")
#Define external binaries
wkhtml_bin = "C:\\Binaries\\wkhtmltopdf.exe"
ghostscript_bin = "C:\\Binaries\\gswin64.exe"

#client_id = "abc"


def validate_client_id(id):
    """
    This function is to validate the client id on given condiiton
    :param client_id: takes the client id to validate
    :return: True/False based
    """
    if id.isdigit() and len(id) == 4:
        return True
    else:
        return False

def delete_files_in_folder(folder_path):
    """
    function that erase existing files in given specific folder path
    :param folder_path: path of folder which needs to be clear
    :return: empty folder
    """

    # Get a list of all files in the folder
    file_list = os.listdir(folder_path)

    # Loop through the files and delete each one
    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted file: {file_name}")
            else:
                print(f"Skipping non-file item: {file_name}")
        except Exception as e:
            print(f"Error while deleting {file_name}: {e}")


def clear_repositories(glb_root):
    """
    this function takes care of all the folders required further in the analysis.
    if the folder do not exist, it create one and erase the folder content
    :param glb_root: root folder for analysis
    :return:
    """

    # Step5: Create working directories if not existing
    try:
        os.mkdir(glb_root)
    except FileExistsError:
        print("Folder already exists. Skipping.")
    try:
        os.mkdir(glb_01_input)
    except FileExistsError:
        print("Folder already exists. Skipping.")

    try:
        os.mkdir(glb_02_master)
    except FileExistsError:
        print("Folder already exists. Skipping.")

    try:
        os.mkdir(glb_03_output)
    except FileExistsError:
        print("Folder already exists. Skipping.")

    try:
        os.mkdir(glb_04_temp_input)
    except FileExistsError:
        print("Folder already exists. Skipping.")

    try:
        os.mkdir(glb_05_mapping_files)
    except FileExistsError:
        print("Folder already exists. Skipping.")

    # Step6:  Clean up of old residue files
    delete_files_in_folder(glb_03_output)
    delete_files_in_folder(glb_04_temp_input)


def check_file_format(file):
    """
    This function takes the file, and check if the file is csv or txt format
    :param file: user imported file or any other file
    :return: True/False
    """
    if file.type == 'text/csv' or file.type == 'text/plain':
        return True
    else:
        return False


def save_file(file, destination):
    with open(destination, 'wb') as out_file:
        out_file.write(file.getbuffer())
        st.write(f"file saved at {destination}")
        time.sleep(3)


def client_file_uploader():
    """
    This function takes user input and saves the imported file
    using client id.
    :return: user uploaded file.
    """

    uploaded_file = st.file_uploader("Upload a genome file here")

    # Check if the file is uploaded
    if check_file_format(uploaded_file):
        st.success("File uploaded successfully")
        # generating file name
        # Extract the file extension
        _, file_extension = os.path.splitext(uploaded_file.name)

        # Define the new filename
        new_file_name = client_id + file_extension

        # Save the uploaded file to the input directory with the new name
        file_path = os.path.join(glb_01_input, new_file_name)

        # saving file
        save_file(uploaded_file, file_path)
        return uploaded_file
    else:
        st.write("enter a valid format.")



def main():
    st.title("Pdf Generator application sample-V01")
    global client_id
    client_id = st.text_input("Enter ID")
    if st.button("Validate ID"):
        if validate_client_id(client_id):
            clear_repositories(glb_root)
            st.write("Folder's set")
            user_file = st.file_uploader("Upload file")
            if user_file is not None:
                st.write("File here")
                time.sleep(4)
            else:
                st.write("upload file")
        else:
            st.error("Invalid Client ID")

if __name__ == "__main__":
    main()
