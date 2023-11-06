# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 12:26:21 2023

@author: sangeetha
"""

########### Stages of scripts for end to end caffeine report generation ###########

### Pre - Stage 1 : Preliminary steps (Importing libraries and creating folder paths)
#### Step1: Import libraries
import pandas as pd
import os
"""
import sys
import subprocess
import numpy as np
from bs4 import BeautifulSoup
import re
from pdfrw import PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from PyPDF2 import PdfMerger

"""
import streamlit as st



#### Step2: Defining folder path
# glb_root = (r"C:\Users\sangeetha\OneDrive\01-XCode\13_scripts\caffeine\working_folder")
glb_root = (r"C:\Users\sangeetha\OneDrive\03_DSM_projects\18_data_apps\Microprojects\13_10_2023_new_master_working_tested")
glb_01_input = (glb_root + r"\01_input")
glb_02_master = (glb_root + r"\02_master")
glb_03_output = (glb_root + r"\03_output")
glb_04_temp_input = (glb_root + r"\04_temp_input")
glb_05_mapping_files = (glb_root + r"\05_mapping_files" + r"\P01_caffeine")
glb_masterfile =  glb_02_master + "\\Caffeine_Master_New.xlsx"
##### For report generation
glb_template_root = (glb_root + r"\06_templates")
#Define external binaries
wkhtml_bin = "C:\\Binaries\\wkhtmltopdf.exe"
ghostscript_bin = "C:\\Binaries\\gswin64.exe"

client_id = "abc"


#HouseKeeping: Clearing up of old residues
# Function to delete all files in the folder path
def delete_files_in_folder(folder_path):
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
            
            
            
            
def clear_repositories():
    #### Step5: Create working directories if not existing
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

        #### Step6:  Clean up of old residue files
    delete_files_in_folder(glb_03_output)
    delete_files_in_folder(glb_04_temp_input)
    
    st.write("Folders emptied!")



def client_file_uploader():
    # File upload section
    uploaded_file = st.file_uploader("Upload a genome file here", type=["csv", "txt"])

    if uploaded_file is not None:
        # Check if the file is uploaded
        
        st.write(client_id)
        st.success("File uploaded successfully")
        
        """

        # Extract the file extension
        _, file_extension = os.path.splitext(uploaded_file.name)

        # Define the new filename
        new_file_name = client_id + file_extension

        # Save the uploaded file to the input directory with the new name
        file_path = os.path.join(glb_01_input, new_file_name)
        st.write(client_id)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getvalue())
            """
            
        

    











def main():
    st.title("XCode Report Generator")
    clear_repositories()
    client_file_uploader()







if __name__=="__main__":
    main()
    
