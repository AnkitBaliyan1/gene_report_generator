#!/usr/bin/env python
# coding: utf-8
###Calling all the required functions to run the script for Caffeine report generation
###import libraries
import pandas as pd
import os
import numpy as np
from bs4 import BeautifulSoup
import re
from pdfrw import PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
import subprocess

# In[2]:

# Defining folder path
# glb_root = (r"C:\Users\sangeetha\OneDrive\01-XCode\13_scripts\caffeine\working_folder")
glb_root = (r"07_10_2023_new_master")
glb_01_input = (glb_root + r"/01_input")
glb_02_master = (glb_root + r"/02_master")
glb_03_output = (glb_root + r"/03_output")
glb_04_temp_input = (glb_root + r"/04_temp_input")
glb_05_mapping_files = (glb_root + r"/05_mapping_files" + r"/P01_caffeine")
glb_masterfile =  glb_02_master + "/Caffeine_Master_New.xlsx"
##### For report generation
glb_template_root = (glb_root + r"/06_templates")
#Define external binaries
wkhtml_bin = "07_10_2023_new_master/External_binary_dependency/wkhtmltopdf.exe"
ghostscript_bin = "07_10_2023_new_master/External_binary_dependency/gswin64.exe"


#wkhtml_bin = "C:\\Binaries\\wkhtmltopdf.exe"
#ghostscript_bin = "C:\\Binaries\\gswin64.exe"

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


# In[3]:


# function to read client file from MyHeritage
def read_file_with_skiprows_MyHeritage(cli_file):
    """
    ####### Works for data generated from MyHeritage #######
    The function reads a file located in the current working directory into a Pandas DataFrame and
    It dynamically determines the number of rows to skip based on the position of the line containing '# rsid'.

    Returns:
        client_raw (DataFrame): The DataFrame containing the file data.
    """

    # Initialize variables
    comments_and_headers = []
    rsid_line_index = None

    # Read the file and store the lines in a list
    with open(cli_file, 'r') as file:
        lines = file.readlines()

    # Find comments and headers and the line containing '# rsid' or 'rsid'
    for i, line in enumerate(lines):
        if line.startswith("#") or line.strip() == "":
            comments_and_headers.append(i)
        elif '# rsid' in line or 'rsid' in line:
            rsid_line_index = i
            break

    # Determine the number of rows to skip (skip comments, headers, and the line with '# rsid' or 'rsid')
    num_rows_to_skip = max(comments_and_headers) + 1 if comments_and_headers else 0

    # Define the data types for specific columns, if needed
    dtypes = {'# rsid': 'str','rsid': 'str', 'RSID': 'str'}
    
    # Read the file into a DataFrame, skipping the determined number of rows
    client_raw = pd.read_csv(cli_file, skiprows=num_rows_to_skip, sep=',', encoding='utf-8', dtype=dtypes, low_memory=False)
    client_raw = client_raw.rename(columns = {'RSID': 'cli_rs_id', 'CHROMOSOME': 'cli_chrm', 'POSITION': 'cli_pos' , 'RESULT': 'cli_geno'})
    return client_raw


# In[4]:


# function to read client file from 23andMe
import pandas as pd

def read_file_with_skiprows_23andme(cli_file):
    """
    ####### Works for data generated from 23andme #######
    The function reads a file located in the current working directory into a Pandas DataFrame and
    It dynamically determines the number of rows to skip based on the position of the line containing '# rsid'.

    Returns:
        client_raw (DataFrame): The DataFrame containing the file data.
    """

    # Initialize variables
    rsid_line_index = None

    # Read the file and store the lines in a list
    with open(cli_file, 'r') as file:
        lines = file.readlines()

    # Find the line containing '# rsid' or 'rsid'
    for i, line in enumerate(lines):
        if line.strip().count('\t') > 0: # Check if line has more than one column (tab-separated)
            if rsid_line_index is None:
                rsid_line_index = i
            else:
                break  # Break the loop once we find a line with more than one column

    # Determine the number of rows to skip (skip lines with only one column before the line with '# rsid' or 'rsid')
    num_rows_to_skip = rsid_line_index if rsid_line_index is not None else 0

    # Define the data types for specific columns, if needed
    dtypes = {'# rsid': 'str', 'rsid': 'str', 'RSID': 'str'}

    # Read the file into a DataFrame, skipping the determined number of rows
    client_raw = pd.read_csv(cli_file, skiprows=range(num_rows_to_skip), sep ='\t', encoding='utf-8', dtype=dtypes, low_memory=False)
    client_raw = client_raw.rename(columns = {'# rsid': 'cli_rs_id', 'chromosome': 'cli_chrm', 'position': 'cli_pos' , 'genotype': 'cli_geno'})
    return client_raw


# In[5]:


# function to read client file from 23andMe
def read_file_with_skiprows_Ances(cli_file):
    """
    ####### Works for data generated from AncestryDNA #######
    The function reads a file located in the current working directory into a Pandas DataFrame and
    It dynamically determines the number of rows to skip based on the position of the line containing '# rsid'.

    Returns:
        client_raw (DataFrame): The DataFrame containing the file data.
    """

    # Initialize variables
    comments_and_headers = []
    rsid_line_index = None

    # Read the file and store the lines in a list
    with open(cli_file, 'r') as file:
        lines = file.readlines()

    # Find comments and headers and the line containing '# rsid' or 'rsid'
    for i, line in enumerate(lines):
        if line.startswith("#") or line.strip() == "":
            comments_and_headers.append(i)
        elif '# rsid' in line or 'rsid' in line:
            rsid_line_index = i
            break

    # Determine the number of rows to skip (skip comments, headers, and the line with '# rsid' or 'rsid')
    num_rows_to_skip = max(comments_and_headers) + 1 if comments_and_headers else 0

    # Define the data types for specific columns, if needed
    dtypes = {'# rsid': 'str', 'rsid': 'str', 'RSID': 'str'}

    # Read the file into a DataFrame, skipping the determined number of rows
    client_raw = pd.read_csv(cli_file, skiprows=num_rows_to_skip, sep='\t', encoding='utf-8', dtype=dtypes, low_memory=False)
    
    # Create new column genotype into client_raw dataframe by joining allele1 and allele2
    client_raw["genotype"] = client_raw["allele1"].astype(str) + client_raw["allele2"].astype(str)
    
    # Create new dataframe in which allele 1 and allele 2 columns are dropped from client_raw dataframe
    new_client_raw = client_raw.drop(columns=["allele1","allele2"])
    new_client_raw = new_client_raw.rename(columns = {'rsid': 'cli_rs_id', 'chromosome': 'cli_chrm', 'position': 'cli_pos' , 'genotype': 'cli_geno'})
    return new_client_raw


# function to read client file with no header (like FTDNA)
def read_file_with_auto_detect(cli_file):
    """
    Read the input file with auto-detection of delimiters and column names.

    Returns:
        client_raw (DataFrame): The DataFrame containing the file data.
    """
    # Initialize variables
    delimiter = None
    
    # Read the file and store the lines in a list
    with open(cli_file, 'r') as file:
        lines = file.readlines()
    line = []    
    # Check if the file contains any of the specified strings
    if not any(keyword in line for keyword in ["AncestryDNA", "23andMe", "MyHeritage"]):
        # If not, read only the first line and convert it to lowercase
        first_line = lines[0].lower()
         
        # Check if the first line contains the required keywords
        if all(keyword in first_line for keyword in ["rsid", "chromosome", "position"]):
            # Read the second line to identify the delimiter
            second_line = lines[1]
            if '\t' in second_line:
                delimiter = '\t'
            elif ',' in second_line:
                delimiter = ','
            else:
                print("Unidentifiable delimiter in the second line.")
                return None

            # Define the data types for specific columns
            dtypes = {'# rsid': 'str', 'rsid': 'str', 'RSID': 'str'}

            # Read the file into a DataFrame using the identified delimiter
            client_raw = pd.read_csv(cli_file, skiprows=1, sep=delimiter, encoding='utf-8', dtype=dtypes, low_memory=False)

            # Manually set the column names to "rsid," "chromosome," "position," and "genotype"
            client_raw.columns = ["rsid", "chromosome", "position", "genotype"]

            return client_raw
        else:
            print("Unidentifiable input file")
            return None
        






# Function to enumerate a folder and get the list of files

def get_file_names_in_path(folder_path):
    file_names = []
    for item in os.listdir(folder_path):
        if item not in [".", ".."]:
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path):
                file_names.append(item)
    return file_names


# In[7]:


# Function to write specific parquet files into a the folder path from the dataframe

def write_dataframes_to_parquet(dataframes_list, filenames_list, target_directory):
    """
    Writes DataFrames to separate Parquet files in the specified target directory.

    Parameters:
        dataframes_list (list): A list of DataFrames.
        filenames_list (list): A list of corresponding filenames for the DataFrames.
        target_directory (str): The path to the directory where the Parquet files will be saved.
    """
    if len(dataframes_list) != len(filenames_list):
        raise ValueError("Number of DataFrames and filenames should be the same.")

    for df, filename in zip(dataframes_list, filenames_list):
        output_path = os.path.join(target_directory, filename)
        df.to_parquet(output_path, index=False)


# In[8]:


# Function to read specific parquet files from the folder path 

def read_specific_parquet_files(directory_path, specific_filenames):
    """
    Reads specific Parquet files from the given directory path into DataFrames.

    Parameters:
        directory_path (str): The path to the directory containing the Parquet files.
        specific_filenames (list): A list of specific filenames to be read.

    Returns:
        dict: A dictionary of DataFrames with filenames as keys and corresponding DataFrames as values.
    """
    dataframes = {}  # A dictionary to store DataFrames with filenames as keys

    for filename in specific_filenames:
        full_path = os.path.join(directory_path, filename)
        if filename in os.listdir(directory_path) and os.path.isfile(full_path):
            df = pd.read_parquet(full_path)
            dataframes[filename] = df
        else:
            print(f"The file '{filename}' does not exist in the directory or is not a valid file.")
            # Handle the case when the file doesn't exist or is not a valid file.

    return dataframes


# In[9]:


# function to extract columns to dataframe

def extract_columns_to_dataframe(dataframe, columns_to_extract):
    """
    Extract specific columns from the DataFrame and create a new DataFrame.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame from which to extract columns.
        columns_to_extract (list or str): The column(s) to extract. If multiple columns, provide them as a list.

    Returns:
        pd.DataFrame: The new DataFrame containing only the extracted columns.
    """
    extracted_dataframe = dataframe[columns_to_extract]
    return extracted_dataframe


# In[10]:


# function to merge two dataframes having same column names as lookup columns

def merge_dataframes_same_colnames(left_df, right_df, on_columns, how='left', keep_only_left=True):
    """
    Merge two DataFrames based on specified columns.

    Parameters:
        left_df (pd.DataFrame): The left DataFrame.
        right_df (pd.DataFrame): The right DataFrame.
        on_columns (list or str): The column(s) to merge on. If multiple columns, provide them as a list.
        how (str, optional): The type of merge to be performed. Defaults to 'left'.
            Options: 'left', 'right', 'outer', 'inner'.
        keep_only_left (bool, optional): If True, keeps only columns from the left DataFrame after the merge.
            If False, renames the columns from the right DataFrame to match the left DataFrame. Defaults to True.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    merged_df = pd.merge(left_df, right_df, on=on_columns, how=how, suffixes=(None, '_y' if keep_only_left else '_x'))
    
    if not keep_only_left:
        # Rename columns from the right DataFrame to match the left DataFrame
        suffix = '_x'
        for col in right_df.columns:
            if col in on_columns:
                continue
            new_col = col + suffix
            while new_col in merged_df.columns:
                suffix += '_'
                new_col = col + suffix
            merged_df.rename(columns={col: new_col}, inplace=True)

    return merged_df


# In[11]:


# function to merge dataframes with lookup columns being different

def merge_dataframes_differ_colnames(left_df, right_df,left_on,right_on, how='left'):
    """
    Merge two DataFrames based on specified columns.

    Parameters:
        left_df (pd.DataFrame): The left DataFrame.
        right_df (pd.DataFrame): The right DataFrame.
        left_on (str or list of str): The column(s) from the left DataFrame to use as merge keys.
        right_on (str or list of str): The column(s) from the right DataFrame to use as merge keys.
        how (str, optional): The type of merge to be performed. Defaults to 'left'.
            Options: 'left', 'right', 'outer', 'inner'.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    merged_df = pd.merge(left_df, right_df, left_on=left_on, right_on = right_on, how=how)
    
    return merged_df        


# In[12]:





# In[13]:


# function to remove duplicates and create a column

def remove_duplicates_and_create_column(source_df, source_column_name, target_df, target_column_name):
    """
    Remove duplicates from the source DataFrame's column and create a new column in the target DataFrame.

    Parameters:
        source_df (DataFrame): The source DataFrame containing the column with duplicates.
        source_column_name (str): The name of the column in the source DataFrame with duplicates.
        target_df (DataFrame): The target DataFrame where the new column will be created.
        target_column_name (str): The name of the new column to be created in the target DataFrame.

    Returns:
        DataFrame: The target DataFrame with the new column containing unique values.
    """
    unique_values = source_df.drop_duplicates(subset=source_column_name, keep='first')[source_column_name]
    target_df[target_column_name] = unique_values.reset_index(drop=True)
    return target_df


###########Report Generation Related Function###########

# Function to remove the specific text
def remove_before_angle_bracket(text):
    if not isinstance(text, str):
        return text
    
    return re.sub(r'^[^<]+', '', text)


# Function to get icon number in Summary pages
def get_icon_number(trait,subheading):
    if not subheading:
        subheading = ""    
    
    subheading = subheading.lower()
    
    if trait == "Caffeine metabolism":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg1_3")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg1_2")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg1_1")
        else:
            return (None, None)
    if trait == "Caffeine sensitivity & smoking":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg1_6")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg1_5")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg1_4")
        else:
            return (None, None)
        
    if trait == "Physical performance & caffeine":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg1_9")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg1_8")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg1_7")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default
    
    if trait == "Caffeine-induced insomnia":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg2_3")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg2_2")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg2_1")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default
        
    if trait == "Caffeine-induced anxiety & panic disorder":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg2_6")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg2_5")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg2_4")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default
        
    if trait == "Caffeine-induced hypertension":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg2_9")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg2_8")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg2_7")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default
        
    if trait == "Caffeine & blood glucose":
        if "lower" in subheading or "fast" in subheading or "Less" in subheading:
            return ("pg3_3")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg3_2")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg3_1")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default
        
    if trait == "Caffeine & heart health":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg3_6")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg3_5")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg3_4")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default   
        
    if trait == "Caffeine & appetite":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg3_9")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg3_8")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg3_7")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default     
        
    if trait == "Caffeine & iron absorption":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg4_3")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg4_2")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg4_1")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default      
        
    if trait == "Caffeine & bone health":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg4_6")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg4_5")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg4_4")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default      
        
    if trait == "Caffeine overconsumption":
        if "lower" in subheading or "fast" in subheading or "less" in subheading:
            return ("pg4_9")
        elif "moderate" in subheading or "moderately" in subheading:
            return ("pg4_8")
        elif "higher" in subheading or "slow" in subheading or "more" in subheading:
            return ("pg4_7")
        else:
            print(f"Unmatched trait: {trait}, subheading: {subheading}")
            return None  # or "default_image_id" if you have a default  
        

#####
def gen_summary_for_file(file_name, rows_to_process):
    # Read the original template
    with open(file_name, "r") as file:
        original_template = file.read()

    # Parse the original template once before the loop
    soup = BeautifulSoup(original_template, 'html.parser')

    for _, row in rows_to_process.iterrows():
        img_id = get_icon_number(row["trait"], row["Subheading"])
        #print(img_id)
        
        # Modify the desired image to make it visible
        image = soup.find("img", {"id": img_id})
        if image:
            image["style"] = "visibility: visible;"

    # Save the final modified HTML after all iterations
    #output_file = file_name.replace(".htm", "_output.html")
    #with open(output_file, "w", encoding='utf-8') as file:
    #    file.write(str(soup))
    with open(glb_03_output + "/allout.html", "a", encoding='utf-8') as file:  # added encoding parameter here
        file.write(str(soup.prettify()))    
        file.write("<div style='page-break-after: always;'></div>")
        file.write(" ")
        file.write(" ")  

# Function to save an individual trait as a separate HTML file
def save_trait_to_file(index, soup):
    filename = f"trait_{index}.html"
    #with open(filename, "w", encoding='utf-8') as file:  # added encoding parameter here
    #    file.write(soup.prettify())
    with open(glb_03_output + "/allout.html", "a", encoding='utf-8') as file:  # added encoding parameter here
#         file.write("<h2 id=page_" + str(index) + "></h2>")
        if index == 0:        
            file.write(f'<h2 id="page_7"></h2>')
        if index == 1:        
            file.write(f'<h2 id="page_8"></h2>')
        if index == 2:        
            file.write(f'<h2 id="page_9"></h2>')
        if index == 3:        
            file.write(f'<h2 id="page_10"></h2>')
        if index == 4:        
            file.write(f'<h2 id="page_11"></h2>')
        if index == 5:        
            file.write(f'<h2 id="page_12"></h2>')
        if index == 6:        
            file.write(f'<h2 id="page_13"></h2>')            
        if index == 7:        
            file.write(f'<h2 id="page_14"></h2>')
        if index == 8:        
            file.write(f'<h2 id="page_15"></h2>')
        if index == 9:        
            file.write(f'<h2 id="page_16"></h2>')
        if index == 10:        
            file.write(f'<h2 id="page_17"></h2>')
        if index == 11:        
            file.write(f'<h2 id="page_18"></h2>')            
        file.write(soup.prettify())    
        file.write("</div>")
        file.write("<div style='page-break-after: always;'></div>")
        file.write(" ")
        file.write(" ")
    #print(f"Saved: {filename}")

def get_bg_color(subheading):
    if not subheading:
        subheading = ""
    
    subheading = subheading.lower()

    if "lower" in subheading or "fast" in subheading or "less" in subheading:
        return "#D5EFC7"
    elif "moderate" in subheading or "moderately" in subheading:
        return "#F6EFC8"
    elif "higher" in subheading or "slow" in subheading or "more" in subheading:
        return "#FFE4E4"
    else:
        return "#FFFFFF"  # default color
    
def get_all_html_files(path):
    """
    Returns a list of all .html files from the provided directory path.
    :param path: Directory path.
    :return: List of paths to .html files.
    """
    all_files = os.listdir(path)
    
    #trait_files = [os.path.join(path, f) for f in all_files if f.startswith('trait_')]
    trait_files = [os.path.join(path, f) for f in all_files if f.startswith('allout.html')]
    summary_html_files = [os.path.join(path, f) for f in all_files if f.endswith('_output.html')]
    
    return trait_files, summary_html_files
    

    
def convert_html_to_pdf(html_path, output_path):
    command = [
                wkhtml_bin, 
                "--enable-local-file-access",
                "--no-outline",
                "--page-size","A4",
                "--margin-right", "10mm",
                "--margin-bottom", "10mm",
                "--margin-left", "10mm",
                "--footer-spacing", "10",
                "--dpi", "300",            
                html_path,
                output_path
    ]
    subprocess.run(command, shell=True)
    
def get_risk_details(subheading):
    if not subheading:
        subheading = ""
    
    subheading = subheading.lower()
    
    if "lower" in subheading or "fast" in subheading or "less" in subheading:
        return ("#008000", "../06_templates/Low1.png")
    elif "moderate" in subheading or "moderately" in subheading:
        return ("#786037", "../06_templates/Moderate1.png")
    elif "higher" in subheading or "slow" in subheading or "more" in subheading:
        return ("#B25F5E", "../06_templates/High1.png")
    else:
        return (None, None)
    




    
