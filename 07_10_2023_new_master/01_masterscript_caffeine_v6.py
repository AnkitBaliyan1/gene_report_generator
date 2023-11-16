#!/usr/bin/env python
# coding: utf-8

########### Stages of scripts for end to end caffeine report generation ###########

### Pre - Stage 1 : Preliminary steps (Importing libraries and creating folder paths)
#### Step1: Import libraries
import pandas as pd
import os
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

#### Step2: Defining folder path
# glb_root = (r"C:\Users\sangeetha\OneDrive\01-XCode\13_scripts\caffeine\working_folder")
glb_root = (r"07_10_2023_new_master")
glb_01_input = (glb_root + r"/01_input")
glb_02_master = (glb_root + r"/02_master")
glb_03_output = (glb_root + r"/03_output")
glb_04_temp_input = (glb_root + r"/04_temp_input")
glb_05_mapping_files = (glb_root + r"/05_mapping_files" + r"/P01_caffeine")
glb_masterfile =  glb_02_master + "//Caffeine_Master_New.xlsx"
##### For report generation
glb_template_root = (glb_root + r"/06_templates")
#Define external binaries
wkhtml_bin = "07_10_2023_new_master/External_binary_dependency/wkhtmltopdf.exe"
ghostscript_bin = "07_10_2023_new_master/External_binary_dependency/gswin64.exe"

# wkhtml_bin = "C:\\Binaries\\wkhtmltopdf.exe"
# ghostscript_bin = "C:\\Binaries\\gswin64.exe"

#### Step3: import functions from functions_master.py
from functions_master_v6 import delete_files_in_folder, read_file_with_skiprows_MyHeritage, read_file_with_skiprows_23andme, read_file_with_skiprows_Ances, read_file_with_auto_detect, get_file_names_in_path, write_dataframes_to_parquet, read_specific_parquet_files, extract_columns_to_dataframe, merge_dataframes_same_colnames, merge_dataframes_differ_colnames,  remove_duplicates_and_create_column, get_icon_number, gen_summary_for_file, save_trait_to_file, get_bg_color, get_all_html_files, convert_html_to_pdf ,remove_before_angle_bracket, get_risk_details

#### Step4: Defining empty dataframe for client file
client_input = pd.DataFrame()

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


## ******* END OF PRE-STAGE1 ********

######### Stage 1: Import and Cleaning input files ############
# - **Import files**
#     1. **Master file**
#         - Caffeine_Master_New.xlsx
#         - We are only interested in "Curation" sheet and "Proxy - European" sheet for this analysis
#     2. **Client_raw file**
#         - txt or csv format files from 23andMe/AncestryDNA/MyHeritage 

#### Step 1: Import Client file from input folder
##### Access input folder
# Define a way to identify the source input files and all the appropriate function to read the input data
# Based on the type of inputs from various companies, we call the functions tuned particularly for the input scenarios
# Read the file and store the lines in a list
client_input.drop(client_input.index, inplace=True)
##################################################################
#Enumerate the 01_input folder for the client files of any format    
##################################################################
file_names_list = get_file_names_in_path(glb_01_input)    
input_file = file_names_list[0]
with open(glb_01_input + "//" + input_file, 'r') as file:
    lines = file.readlines()
       
# Reading lines from client raw files based on sources as defined in the functions 
for i, line in enumerate(lines):
    if  "AncestryDNA" in line:
        client_input = read_file_with_skiprows_Ances(glb_01_input + "//" + input_file)
        break
    elif "23andMe" in line:
        client_input = read_file_with_skiprows_23andme(glb_01_input + "//" + input_file)
        break
    elif "MyHeritage" in line:
        client_input = read_file_with_skiprows_MyHeritage(glb_01_input + "//" + input_file)
        break
    else:
        # If none of the keywords are found, use auto-detection function
        client_input = read_file_with_auto_detect(glb_01_input + "//" + input_file)
        if client_input is not None:
            break

#### Step 2: Import Caffeine Master from master folder
#import 'Curation' sheet from Caffeine Master into cur_mast datafame
cur_mast= pd.read_excel(glb_masterfile,sheet_name = "Curation")
### Step 3: Import Proxy-European sheet in caffeine master
#import '"Proxy - European"' sheet from Caffeine Master into proxy_mast datafame
proxy_mast= pd.read_excel(glb_masterfile,sheet_name = "Proxy - European")
#### Step 4: Rename Columns
# 1. Open **"Curation sheet"** in the dataframe **cur_mast**: 
#     - Rename each column in the dataframe with short "cur-" prefix
# 2. Open **"Proxy-European sheet"** in the dataframe **proxy_mast**: 
#     - Rename each column in the dataframe with short "prxy-" prefix:
#Rename columns in cur_mast
cur_mast.rename(columns={
    "trait_class": "cur_trt_clss",
    "trait": "cur_trt",
    "rsid": "cur_rs_id",
    "gene": "cur_gene",
    "na": "cur_na",
    "va": "cur_va",
    "association": "cur_asn",
    "effect": "cur_effect",
    "score": "cur_scortyp",
    "annotation": "cur_anno",
    "ref": "cur_ref",
    "verification": "cur_verifcn",
    "comment": "cur_comment"
}, inplace=True)

#Rename columns in proxy_mast
proxy_mast.rename(columns={
    "rsID": "prxy_rs_id",
    "proxy-rsID": "prxy_prx_rs_id",
    "chromsome": "prxy_chrm",
    "position - rsID": "pos_rs_id",
    "position - proxy": "pos_prxy",
    "distance": "prxy_dist",
    "R2": "prxy_r2",
    "major allele": "prxy_mjr_alle",
    "minor allele": "prxy_mnr_alle",
    "MAF": "prxy_maf"
}, inplace=True)

#### Step 5: Select Relevant Columns to clean data
#  1. Create **new curation dataframe** with only the columns that are used for analysis
#  2. Create **new Proxy_European sheet** with only columns that are used for analysis
#  3. Create **new client file** with only columns that are used for analysis
#Create new curation dataframe with only relavant columns
new_cur_mast = cur_mast[["cur_trt_clss","cur_trt","cur_rs_id", "cur_gene", "cur_na", "cur_va", "cur_effect", "cur_scortyp"]]
#Create new proxy dataframe with only relavant columns
new_prxy_mast = proxy_mast[["prxy_rs_id", "prxy_prx_rs_id", "prxy_mjr_alle", "prxy_mnr_alle"]]
#Rename columns in client_raw
client_input.rename(columns={"rsid": "cli_rs_id", "chromosome": "cli_chrm", "position": "cli_pos", "genotype": "cli_geno"}, inplace=True)
#Create new client file dataframe with only relavant columns
new_clnt_rw =client_input[["cli_rs_id","cli_geno"]]
# print(new_clnt_rw)
#### Step 6: Export to Temp Input
# 1. Export the new curation dataframe 'new_cur_mast' as output into Temp input folder
# 2. Export new proxy dataframe 'new_prxy_mast' as output into Temp input folder
# 3. Export client file dataframe 'new_clnt_rw' as output into Temp input folder
# Form a list of dataframes to be converted to parquet files
dataframes = [new_cur_mast,new_prxy_mast,new_clnt_rw]
# Form a list of file names in parquet format
filenames = ['new_cur_mast.parquet', 'new_prxy_mast.parquet', 'new_clnt_rw.parquet']
# Call the function to write the DataFrames to Parquet files
write_dataframes_to_parquet(dataframes, filenames, glb_04_temp_input)
# ******* END OF STAGE1 ********

######################Stage 2: Input new files from Stage 1 and perform MATCHES ##########################
#### Step1: Input files as dataframes into the environment 
# - input curation master : **new_cur_mast**
# - input proxy file: **new_prxy_mast**
# - input client raw data: **new_clt_rw**
specific_filenames = ['new_cur_mast.parquet', 'new_prxy_mast.parquet', 'new_clnt_rw.parquet']
# Call the function to read specific Parquet files
dataframes = read_specific_parquet_files(glb_04_temp_input, specific_filenames)
new_cur_mast = dataframes['new_cur_mast.parquet']
new_prxy_mast = dataframes['new_prxy_mast.parquet']
new_clnt_rw = dataframes['new_clnt_rw.parquet']
#### Step 2: Create Client_proxy_match file having matched proxy's for client rsid not in curation
# - First Dataframe: **Cl_pr_match** (Exported in same name as Parquet)
#     - Column1: **prxy_rs_id**: same as "rsID" in "Proxy-European"
#     - Column2: **prxy_prx_rs_id**: same as "proxy-rsID" in "Proxy-European"
#     - Column3: **cust_match**: Gives 1 when proxy found in client and 0 not found
# Columns to extract
columns_to_extract = ["prxy_rs_id", "prxy_prx_rs_id"]
cl_pr_match = extract_columns_to_dataframe(new_prxy_mast,columns_to_extract)
cl_pr_match = cl_pr_match.copy()
#The proxy rsid column in  cl_pr_match dataframe is matched with the cli_rs_id column in client raw file and column is generated with 1 or 0 indicating match and mismatch
#astype function converts boolean values into integers 0 or 1
cl_pr_match['cust_match'] = 0  # Initialize the 'cust_match' column with 0
cl_pr_match.loc[cl_pr_match["prxy_prx_rs_id"].isin(new_clnt_rw["cli_rs_id"]), 'cust_match'] = 1
# - Second Dataframe: **Clnt_prx_mtch1** (Exported in same name as Parquet)
#     - Has only the matched proxy IDs (i.e., rows with only '1s' in cust_match from Cl_pr_match)
#     - Column1: **prxy_rs_id**
#     - Column2: **prxy_prx_rs_id**
#     - Column3: **prxy_mjr_allele**: corresponding proxy major allele (Proxy_NA)
#     - Column4: **prxy_mnr_allele**: corresponding proxy minor allele (Proxy_VA)
# Dataframe clnt_prx_mtch1 contains those prxy_rs_id and prxy_prx_rs_id from cl_pr_match that has cust_match to be 1
clnt_prx_mtch1 = cl_pr_match.loc[cl_pr_match['cust_match']==1,['prxy_rs_id','prxy_prx_rs_id']]
merge_on_columns = ['prxy_rs_id', 'prxy_prx_rs_id']       
# Call the function to merge the DataFrames
clnt_prx_mtch1  = merge_dataframes_same_colnames(clnt_prx_mtch1, new_prxy_mast, merge_on_columns, how='left')
# Form a list of dataframes to be converted to parquet files
dataframes = [cl_pr_match,clnt_prx_mtch1]
# Form a list of file names in parquet format
filenames = ['cl_pr_match.parquet', 'clnt_prx_mtch1.parquet']
# Call the function to write the DataFrames to Parquet files
write_dataframes_to_parquet(dataframes, filenames, glb_04_temp_input)
#### Step 3: Curation_trait_match (to use in Stage 4)
# - Create a new Dataframe: cur_rs_trt_match
# - Is a match between **curation_rsid** and **trait_class** and create a unique_id for each rsid
# - It filters out all the curation rs_id from "Curation sheet" as it is
columns_to_extract = ["cur_rs_id","cur_na","cur_va"]
cur_rs_trt_match = extract_columns_to_dataframe(new_cur_mast,columns_to_extract)
# #Create a dataframe cur_rs_trt_match with the curation_rsid and trait_class column from the new_prxy_mast dataframe
dataframes = [cur_rs_trt_match]
# Form a list of file names in parquet format
filenames = ['cur_rs_trt_match.parquet']
# Call the function to write the DataFrames to Parquet files
write_dataframes_to_parquet(dataframes, filenames, glb_04_temp_input)
##### ******* END OF STAGE2 ******** ########

###############################Stage 3: Creating of combined rsid and direct inputs from "Curation master"########################
#### Step 1: Match rsids in Client raw file with rsids in Curation master
left_on_columns = ["cur_rs_id"]
right_on_columns = ["cli_rs_id"]   
# Call the function to merge the DataFrames
Cur_cli_rsid_mtch  = merge_dataframes_differ_colnames(new_cur_mast, new_clnt_rw, left_on_columns, right_on_columns, how='left')
#### Step 2: Create dataframe with only rsids (QC_rsids) using Cur_cli_rsid_match
#Create a dataframe QC_rsids with only the rsid columns from Cur_cli_rsid_match
columns_to_extract = ["cur_rs_id", "cli_rs_id"]
QC_rsids = extract_columns_to_dataframe(Cur_cli_rsid_mtch,columns_to_extract)
QC_rsids = QC_rsids.copy()
#Fill with "NA" where ever the match between rsids were not found
QC_rsids= QC_rsids.fillna("NA")
#create the proxy_rs_id column
QC_rsids["proxy_rs_id"]=''
# Iterate over each row in QC_rsids
for index, row in QC_rsids.iterrows():
    # Check if client_rs_id value is "NA"
    if row['cli_rs_id'] == 'NA':
        Curation_rs_id = row['cur_rs_id']
        # Look up corresponding value of Curation_rs_id in prxy_rs_id of clnt_prx_mtch1
        match_value = clnt_prx_mtch1.loc[clnt_prx_mtch1['prxy_rs_id']== Curation_rs_id, "prxy_prx_rs_id"].values
        # If a match is found, assign corresponding value from prxy_prx_rs_id of clnt_prx_mtch1 to proxy_rs_id of QC_rsids
        if len(match_value) >0:
            QC_rsids.at[index,'proxy_rs_id']= match_value[0]
        else:
            QC_rsids.at[index,'proxy_rs_id']= "No Proxy"
#### Step 3: Create combined rsids having both curation and proxy_ rsids
# create the combnd_rs_id column
QC_rsids["combnd_rs_id"]=''
# Iterate over each row in df1
for index, row in QC_rsids.iterrows():
    if row['proxy_rs_id'] == "":
        QC_rsids.at[index, 'combnd_rs_id'] = row['cli_rs_id']
    else:
        QC_rsids.at[index, 'combnd_rs_id'] = row['proxy_rs_id']
#### Step 4: Create unique_IDs 
# create the unique_rs_id column
QC_rsids["rs_uniq_id"]=''
QC_rsids["trait_class"] = new_cur_mast["cur_trt_clss"]
# Create a list to keep track of the count for each unique value in combnd_rs_id column
count_list = []
## Initialize counter
no_proxy_count = 0
for index, row in QC_rsids.iterrows():
    combnd_rs_id = row["combnd_rs_id"]
    cli_rs_id = row["cli_rs_id"]
    trait_class = row["trait_class"]
    
    # Condition 1: If "combnd_rs_id" column has "No Proxy", fill "No Proxy" in "rs_uniq_id"
    if combnd_rs_id == "No Proxy":
        no_proxy_count += 1
        QC_rsids.at[index, "rs_uniq_id"] = "No Proxy" + str(no_proxy_count)

    else:
        # Condition 2: If "cli_rs_id_" column has 'NA', form unique id with "|T", "NA", and count of occurrence
        if cli_rs_id == "NA":
            count= count_list.count(combnd_rs_id) + 1
            count_list.append(combnd_rs_id)
            unique_id = f"{combnd_rs_id}|T{trait_class}|NA{count}"
            QC_rsids.at[index, "rs_uniq_id"] = unique_id
            
        # Condition 3: If "cli_rs_id_" column is not 'NA', form unique id with "|T" and count of occurrence
        else:
            count= count_list.count(combnd_rs_id) + 1
            count_list.append(combnd_rs_id)
            unique_id = f"{combnd_rs_id}|T{trait_class}|{count}"
            QC_rsids.at[index, "rs_uniq_id"] = unique_id
#### Step 5: Create first version of QC file (QC_file1)
#Copy the contents of QC_rsids into QC_file1
QC_file1 = QC_rsids.copy()
#Copy the direct contents of trait class, gene, effect and scoretype from curation into the QC_file1
QC_file1 = pd.concat([QC_file1,new_cur_mast[['cur_trt','cur_gene','cur_effect', 'cur_scortyp']]],axis =1)
#Rename the columns as supposed to in QC_file1
QC_file1 = QC_file1.rename(columns={'cur_trt':'trait','cur_gene': 'gene','cur_effect':'effect_b_or_r','cur_scortyp':'scoretype'})
###### ******* END OF STAGE3 ********

########################Stage 4: Filling in Proxy, NA and VA into QC file2##################################
#### Step 1: Create Proxy column in QC file
#Create a column called proxy in QC_file1
QC_file1.loc[:,'Proxy'] = ''
# Apply the condition to create Proxy column
QC_file1.loc[QC_file1['proxy_rs_id'] =='','Proxy'] = "N"
QC_file1.loc[QC_file1['proxy_rs_id'] !='','Proxy'] = "Y"

#### Step 2: NA or major allele and VA or minor allele in one column in QC_file
#### a. Add the unique rsid column in curation_trait_match file
cur_rs_trt_match = cur_rs_trt_match.copy()
#Add the unique rsid column to curation_trait_match file
cur_rs_trt_match["cur_uniq_id"]= QC_file1["rs_uniq_id"]

#### b. Create a new NA-VA dataframe with only major allele and minor allele
#  - NA-VA has only proxy NA and proxy VA for corresponding proxy_rsid
# Create the NA_VA dataframe with the specified columns
NA_VA = pd.DataFrame(columns=['prxy_rs_id', 'rs_uniq_id', 'NA', 'VA'])
# Get the proxy rsids into NA_VA by filtering the client rsid with 'NA' from QC_file2
filtered_values = QC_file1.loc[QC_file1['cli_rs_id'] == 'NA','proxy_rs_id']
NA_VA['prxy_rs_id'] = filtered_values.reset_index(drop=True)
NA_VA = NA_VA[NA_VA['prxy_rs_id'].notnull()]
# Get the unique rsids into NA_VA by filtering the client rsid with 'NA' from QC_file2
filtered_values = QC_file1.loc[QC_file1['cli_rs_id'] == 'NA', 'rs_uniq_id']
NA_VA['rs_uniq_id'] = filtered_values.reset_index(drop=True)
# Create a dictionary mapping 'prxy_prx_rs_id' to 'prxy_mjr_alle'
mapping_dict_na = clnt_prx_mtch1.set_index('prxy_prx_rs_id')['prxy_mjr_alle'].to_dict()
# Map the values from 'proxy_rs_id' to create the 'NA' column in NA_VA DataFrame and replace NaN values with 'No Proxy'
NA_VA['NA'] = NA_VA['prxy_rs_id'].map(mapping_dict_na).fillna("No Proxy")
# Create a dictionary mapping 'prxy_prx_rs_id' to 'prxy_mjr_alle'
mapping_dict_va = clnt_prx_mtch1.set_index('prxy_prx_rs_id')['prxy_mnr_alle'].to_dict()
# Map the values from 'proxy_rs_id' to create the 'VA' column in NA_VA DataFrame and replace NaN values with 'No Proxy'
NA_VA['VA'] = NA_VA['prxy_rs_id'].map(mapping_dict_va).fillna("No Proxy")
#Create nonprxy_na_va with specified columns
nonprxy_na_va = pd.DataFrame(columns=['cur_rs_id', 'rs_uniq_id', 'NA', 'VA'])
# Into the 'curation_rs_id' column of nonprxy_na_va, filtered values of client rsid without NA is added
filtered_values = QC_file1.loc[QC_file1['cli_rs_id'] != 'NA','cur_rs_id']
nonprxy_na_va['cur_rs_id'] = filtered_values.reset_index(drop=True)
# Into the 'unique_id' column of nonprxy_na_va, filtered values of client rsid without NA is added
filtered_values = QC_file1.loc[QC_file1['cli_rs_id'] != 'NA','rs_uniq_id']
nonprxy_na_va['rs_uniq_id'] = filtered_values.reset_index(drop=True)
# - nonprxy_na_va: NA column 
# Create a dictionary mapping 'cur_uniq_id' to 'cur_na'
mapping_dict_na1 = cur_rs_trt_match.set_index('cur_uniq_id')['cur_na'].to_dict()
# Map the values from 'rs_uniq_id' to create the 'na' column in nonprxy_na_va DataFrame and replace NaN values with 'No Proxy'
nonprxy_na_va['NA'] = nonprxy_na_va['rs_uniq_id'].map(mapping_dict_na1).fillna("No Proxy")
# Create a dictionary mapping 'cur_uniq_id' to 'cur_na'
mapping_dict_va1 = cur_rs_trt_match.set_index('cur_uniq_id')['cur_va'].to_dict()
# Map the values from 'rs_uniq_id' to create the 'na' column in nonprxy_na_va DataFrame and replace NaN values with 'No Proxy'
nonprxy_na_va['VA'] = nonprxy_na_va['rs_uniq_id'].map(mapping_dict_va1).fillna("No Proxy")
# Concatenate the unique IDs and corresponding NA and VA values from both na_va and nonprxy_na_va dataframes
NA_VA_combi = pd.concat([nonprxy_na_va[['rs_uniq_id', 'NA', 'VA']], NA_VA[['rs_uniq_id', 'NA', 'VA']]], ignore_index=True)
merge_on_columns = ["rs_uniq_id"]
# Call the function to merge the DataFrames
QC_file1  = merge_dataframes_same_colnames(QC_file1, NA_VA_combi[['rs_uniq_id', 'NA', 'VA']], merge_on_columns, how='left')
## ******* END OF STAGE4 ********

###################Stage 5: Create Complement column##############################
file_name = "tbl_lup_complement.csv"
file_path = os.path.join(glb_05_mapping_files, file_name)
tbl_lup_complement = pd.read_csv(file_path, engine='python')
#Create a new column 'complement' in QC_files2 by looking up for concatenated NA and VA in tbl_lup_complement
QC_file1['complement'] = QC_file1.apply(lambda row: tbl_lup_complement.loc[(tbl_lup_complement['NA/VA'] == row['NA'] + row['VA']), 'Value'].values[0] if any(tbl_lup_complement['NA/VA'] == row['NA'] + row['VA']) else 'N', axis=1)
## ******* END OF STAGE5 ********

################### Stage 6: Genotype and Alleles columns into QC_file1#################################
# print(left_on_columns)
left_on = "combnd_rs_id"
right_on = "cli_rs_id"
# Reset the index of 'new_cli_rw' DataFrame before performing the merge
# new_clnt_rw_reset = new_clnt_rw.reset_index().rename(columns={'cli_rs_id': 'new_cli_rs_id'})
#new_clnt_rw[['cli_geno']]
left_on_columns
right_on_columns
result_df = merge_dataframes_differ_colnames(QC_file1, new_clnt_rw, left_on, right_on, how='left')
QC_file1['genotype'] = result_df['cli_geno'].fillna("No Proxy")
# Create new allele1 column by extracting first character
QC_file1['allele_1'] = QC_file1['genotype'].str[0] 
# Create new allele2 column by extracting second character
QC_file1['allele_2'] = QC_file1['genotype'].str[1] 
# Replace 'No Proxy' with 'NA'
QC_file1.loc[(QC_file1['genotype'] == 'No Proxy')| (QC_file1['genotype'] =='--'), ['allele_1', 'allele_2']] = 'NA'
## ******* END OF STAGE6 ********

#####################Stage 7:Switching: switch and va_switched columns in QC_file1##################
### Step 1: Switch column
###### input tbl_lup_switch file
file_name = "tbl_lup_switch.csv"
file_path = os.path.join(glb_05_mapping_files, file_name)
tbl_lup_switch = pd.read_csv(file_path, engine='python')
# Lookup function to check if concatenated value exists in tbl_lup_switch
def lookup_switch(row):
    concatenated_value = f"{row['VA']}_{row['complement']}_{row['allele_1']}_{row['allele_2']}"
    if concatenated_value in tbl_lup_switch['Switch'].values:
        return 'Y'
    else:
        return 'N'
# Create a new column 'switch' in QC_file3 based on the lookup function
QC_file1['switch'] = QC_file1.apply(lookup_switch, axis=1)
#### Step 2: VA_switched column
###### input tbl_lup_swi_va
file_name = "tbl_lup_swi_va.csv"
file_path = os.path.join(glb_05_mapping_files, file_name)
tbl_lup_swi_va = pd.read_csv(file_path, engine='python')
left_on = QC_file1['switch'] + QC_file1['VA']
right_on='Switch_VA'
# Merge the dataframes based on concatenated 'switch' and 'VA' columns
merged_df = merge_dataframes_differ_colnames(QC_file1, tbl_lup_swi_va, left_on, right_on, how='left')
# Create the 'va_switched' column in QC_file3 based on the merge result
QC_file1['va_switched'] = merged_df['Value'].fillna('NA')
#### Step 3: Creating combnd_va in QC_file1
# Create the 'combnd_va' column in QC_file3 based on 'va_switched' and 'VA' values
QC_file1['combnd_va'] = QC_file1.apply(lambda x: x['VA'] if x['va_switched'] == 'NA' else x['va_switched'], axis=1)
# ******* END OF STAGE7 ********

#################Stage 8:Generating scores##############################
#### Step 1: Create condition columns for scores in QC_file1
# Create the 'VA_A1' column in QC_file1 based on 'va_switched' and 'VA' values
QC_file1['VA_A1'] = QC_file1.apply(lambda x: 1 if x['VA'] == x['allele_1'] else 0, axis=1)
# Create the 'VA_A2' column in QC_file1 based on 'va_switched' and 'VA' values
QC_file1['VA_A2'] = QC_file1.apply(lambda x: 1 if x['VA'] == x['allele_2'] else 0, axis=1)
# Create the 'VA_A2' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['A1_A2'] = QC_file1.apply(lambda x: 1 if x['allele_1'] == x['allele_2'] else 0, axis=1)
# Create the 'VA_NoPrxy' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['VA_NoPrxy'] = QC_file1.apply(lambda x: 1 if x['VA'] == "No Proxy" else 0, axis=1)
# Create the 'Geno_--' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['Geno_--'] = QC_file1.apply(lambda x: 1 if x['genotype'] == "--" else 0, axis=1)
# Create the 'eff_R' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['eff_R'] = QC_file1.apply(lambda x: 1 if x['effect_b_or_r'] == "R" else 0, axis=1)
# Create the 'eff_B' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['eff_B'] = QC_file1.apply(lambda x: 1 if x['effect_b_or_r'] == "B" else 0, axis=1)
# Create the 'swi_y' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['swi_y'] = QC_file1.apply(lambda x: 1 if x['switch'] == "Y" else 0, axis=1)
# Create the 'sco_ty_x' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['sco_ty_x'] = QC_file1.apply(lambda x: 1 if x['scoretype'] == "X" else 0, axis=1)
# Create the 'sco_ty_y' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['sco_ty_y'] = QC_file1.apply(lambda x: 1 if x['scoretype'] == "Y" else 0, axis=1)
# Create the 'sco_ty_z' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['sco_ty_z'] = QC_file1.apply(lambda x: 1 if x['scoretype'] == "Z" else 0, axis=1)
file_name = "tbl_lup_scores.csv"
file_path = os.path.join(glb_05_mapping_files, file_name)
tbl_lup_scores = pd.read_csv(file_path, engine='python')
QC_file1['combnd_score'] = QC_file1.apply(lambda row: tbl_lup_scores.loc[(tbl_lup_scores['Lup'] == "SK" + str(row['VA_A1']) + str(row['VA_A2']) + str(row['A1_A2']) + str(row['VA_NoPrxy']) + str(row['Geno_--']) + str(row['eff_R']) + str(row['eff_B']) + str(row['swi_y']) + str(row['sco_ty_x']) + str(row['sco_ty_y']) + str(row['sco_ty_z'])), 'value'].values[0] if any(tbl_lup_scores['Lup'] == "SK" + str(row['VA_A1']) + str(row['VA_A2']) + str(row['A1_A2']) + str(row['VA_NoPrxy']) + str(row['Geno_--']) + str(row['eff_R']) + str(row['eff_B']) + str(row['swi_y']) + str(row['sco_ty_x']) + str(row['sco_ty_y']) + str(row['sco_ty_z'])) else 'X', axis=1)
QC_file1.to_parquet(glb_04_temp_input + '\\QC_file1.parquet', index=False)
QC_file1.to_csv(glb_04_temp_input + '\\QC_file1.csv', index=False)
QC_output = QC_file1[['trait_class','trait','combnd_rs_id','gene','Proxy','effect_b_or_r','switch','scoretype','genotype','allele_1','allele_2','combnd_score']]
print(QC_output)
## ******* END OF STAGE8 ********

##################Stage 9: Generating mean score ########################## 
#### Step 1: Average score for each trait class
#Convert all scores in QC_file1 to numerical (float)
QC_file1['combnd_score'] = pd.to_numeric(QC_file1['combnd_score'], errors='coerce')
#### Create QC_file2 having trait information without duplicates 
columns = ['trait_class', 'trait']
QC_file2= pd.DataFrame(columns=columns)
QC_file2 = remove_duplicates_and_create_column(QC_file1, 'trait_class', QC_file2, 'trait_class')
QC_file2 = QC_file2.sort_values(by='trait_class', ascending=True)
QC_file2  = remove_duplicates_and_create_column(QC_file1, 'trait', QC_file2, 'trait')
###### Calculate mean score for each trait class and implement it into QC_file2
# Calculate the mean of 'combnd_score' for each 'trait_class' in QC_file1
average_scores = QC_file1.groupby('trait')['combnd_score'].mean().reset_index()
average_scores['combnd_score'] = average_scores['combnd_score'].round(2)  # Round to the nearest multiple of 2
# Step 3: Merge the average_scores with QC_file5 based on the 'trait' column
QC_file2 = QC_file2.merge(average_scores, on='trait', how='left')
# Rename combnd score column in QC_file2 to mean_score
QC_file2 = QC_file2.rename(columns={'combnd_score': 'mean_score'})
# the QC_file3 dataframe is exported into the temp input folder
QC_file2.to_parquet(glb_04_temp_input + '\\QC_file2.parquet', index=False)

#print("mean_score")
#print(QC_file2)

###############Stage 10: Generate Outcomes and Recommendations#######################
#import the QC_file1 into QC_file4 from stage 8
QC_file1 = pd.read_parquet(glb_04_temp_input + '\\QC_file1.parquet')
#import the QC_file5 from stage 9
QC_file2 = pd.read_parquet(glb_04_temp_input + '\\QC_file2.parquet')
#import 'Curation' sheet from Caffeine Master into cur_mast datafame
outcome_recommd = pd.read_excel(glb_02_master + '//Caffeine_Master_New.xlsx', sheet_name = "Outcomes and Recommendations")
# Add the "trait_class" column
outcome_recommd.insert(0, 'trait_class', range(1, 13))
# Columns to transform with hardcoded bounds
cols_to_transform = {
    "0.66 (slow metabolizer - high risk)": (0, 0.66),
    "0.67-1.33 (moderate metabolizer - moderate risk)": (0.67, 1.33),
    "1.34 (fast metabolizer - low risk)": (1.34, 10)
}
new_data = []

# iterate over each specified column
for col, bounds in cols_to_transform.items():
    # extract the outcomes
    outcome = re.findall(r"\((.*?)\)", col)[0]

    # extract the hardcoded bounds
    lower, upper = bounds

    # iterate over each row in the column
    for i in outcome_recommd.index:
        trait_class = outcome_recommd.loc[i, 'trait_class']
        recommendation = outcome_recommd.loc[i, col]
        trait_name = outcome_recommd.loc[i, 'trait name']  # assuming 'trait_name' is the other column you want to keep
        description = outcome_recommd.loc[i, 'trait description']
        genes_analyzed = outcome_recommd.loc[i, 'genes']
        gene_markers_analyzed = outcome_recommd.loc[i, 'num_markers']
        
        # append the extracted data to new_data
        new_data.append([trait_class, lower, upper, outcome, recommendation, trait_name, description, genes_analyzed, gene_markers_analyzed])

# create a new DataFrame from new_data
mapping_df = pd.DataFrame(new_data, columns=['trait_class', 'ref_score_lower', 'ref_score_upper', 'outcomes', 'recommendations', 'trait_name','description', 'genes_analyzed', 'gene_markers_analyzed'])

# sort the DataFrame based on 'trait class' and reset the index
mapping_df = mapping_df.sort_values(by='trait_class').reset_index(drop=True)

# Extracting the text
# pattern = r'\] (.*?)(?= =)|\](?:[^\:]*\:)([^\:]*)(?=:)'
pattern = r'(Lower|Slow|Fast|Moderate|Higher):(.*?)(?=\=)'
mapping_df['Subheading'] = mapping_df['recommendations'].str.extract(pattern).fillna('').sum(axis=1).str.strip()
#print ("Mapping DF>>")
#print(mapping_df)
# value_map = {'Caffeine sensitivity': 'Caffeine metabolism', 'Sprint activity/ Sprinting performance-enhancing effects of caffeine': 'Physical performance and caffeine', 'caffeine and appetite': 'Caffeine and appetite', 'High consumption':'Caffeine overconsumption'}
# QC_file5['trait'] = QC_file5['trait'].replace(to_replace=value_map)
QC_file6 = QC_file2.copy()

def assign_values(row):
    # Filter mapping_df for the specific trait_class
    subset = mapping_df[mapping_df['trait_class'] == row['trait_class']]
    
    # Check if the mean_score falls within the range for any of the rows in the subset
    mask = (subset['ref_score_lower'] <= row['mean_score']) & (subset['ref_score_upper'] >= row['mean_score'])
    
    # If there's a match, return the corresponding outcome
    if mask.sum() > 0:
        matched_row = subset[mask].iloc[0]
        return pd.Series({
            'recommendations': matched_row['recommendations'],
            'description': matched_row['description'],
            'genes_analyzed': matched_row['genes_analyzed'],
            'gene_markers_analyzed': matched_row['gene_markers_analyzed'],
            'Subheading': matched_row['Subheading']
            })
    else:
        return pd.Series({
            'recommendations': np.nan,
            'description': np.nan,
            'genes_analyzed': np.nan,
            'gene_markers_analyzed': np.nan,
            'Subheading': np.nan
        })
    
outputs = QC_file6.apply(assign_values, axis=1)
QC_file6 = pd.concat([QC_file6, outputs], axis=1)
#Bring gene_markers in your genome column from QC_file4
QC_file1["cli_count"] = np.where(QC_file1['cli_rs_id'] !='NA',1, 0)
agg_data = QC_file1.groupby('trait_class')['cli_count'].sum().reset_index()
# Get the client_markers into the QC_file6
QC_file6 = pd.merge(QC_file6, agg_data, on='trait_class', how='left')
QC_file6.rename(columns={'cli_count': 'cli_markers'}, inplace=True)
# the QC_file3 dataframe is exported into the temp input folder
QC_file6.to_parquet(glb_04_temp_input + '\\QC_file2.parquet', index=False)

#####################################################
#####################################################
#REPORT GENERATION
#####################################################
#####################################################
QC_file6= pd.read_parquet(glb_04_temp_input + '\\QC_file2.parquet')

#print (QC_file6['Subheading'][0])

# Add a space after each comma in the "genes analysed" column
QC_file6['genes_analyzed'] = QC_file6['genes_analyzed'].str.replace(',', ', ')
# Assuming QC_file5 is your DataFrame and 'recommendations' is your column name
QC_file6['recommendations'] = QC_file6['recommendations'].apply(remove_before_angle_bracket)


# print(QC_file6[['trait','Subheading']])
df = QC_file6
# Read the TOC template from the file
with open(glb_template_root + "//toc_template.html", "r") as file:
    original_toc_template = file.read()

#Function to process summary pages as html
def process_all_files():
    files = [glb_template_root + "//Batch_2_1_v3.htm", glb_template_root + "//Batch_2_2_v3.htm", glb_template_root + "//Batch_2_3_v3.htm", glb_template_root + "//Batch_2_4_v3.htm"]

    # This assumes you always process 3 rows for each file. Adjust as necessary.
    chunks = [df.iloc[i:i+3] for i in range(0, len(df), 3)]
    
    for file_name, rows_to_process in zip(files, chunks):
        gen_summary_for_file(file_name, rows_to_process)      

######### Create new allout html 
with open(glb_03_output + "//allout.html", "w", encoding='utf-8') as file:
    file.write(original_toc_template)
    file.write(" ")  

process_all_files()

#Read the template from the file
with open(glb_template_root + "//report_long_template.htm", "r") as file:
    original_template = file.read()

#Read the Disclaimer template from the file
with open(glb_template_root + "//disclaimer_template.html", "r", encoding="utf-8") as file:
    original_disclaimer_template = file.read() 


from bs4 import BeautifulSoup
#Font sizes - you can adjust these values as per your requirement
trait_font_size = "22px"
subheading_font_size = "20px"
description_font_size = "17px"
recommendations_font_size = "28px"

#Loop through the DataFrame rows and create individual HTML files
page_count=7
#for index, row in df.iterrows():
for index, row in df.iloc[:-1].iterrows():
    # Parse the original template for each row so we start fresh
    soup = BeautifulSoup(original_template, 'html.parser')
    
     # Extract color and image based on subheading
    color, risk_image = get_risk_details(row['Subheading'])

    # print ("lastline>>" + row['Subheading'])
    #sys.exit();

    # Create the nested table for details
    nested_table = soup.new_tag("table", id="inner_table")

    # Create first row for tick, trait, and subheading
    tr1 = soup.new_tag("tr")

    # Cell for risk symbol (image)
    td1 = soup.new_tag("td", id="image_cell")
    tick_img = soup.new_tag("img", src=risk_image, alt="Image Description")
    td1.append(tick_img)
    tr1.append(td1)

    # Cell for trait (in bold) and subheading (below the trait)
    td2_style = f"line-height: 1.5; background-color: {get_bg_color(row['Subheading'])};"
    td2 = soup.new_tag("td", id="content_cell", style=td2_style)
    bold_trait = soup.new_tag("strong", style=f"font-size: {trait_font_size};")
    bold_trait.string = row["trait"]
    td2.append(bold_trait)
    td2.append(soup.new_tag("br"))
    subheading = soup.new_tag("span", style=f"font-size: {subheading_font_size};")
    print(">>>>")
    print(row["Subheading"])
    subheading.string = row["Subheading"]
    td2.append(subheading)
    tr1.append(td2)
    nested_table.append(tr1)

    # Create second row for description
    tr2 = soup.new_tag("tr")
    td3 = soup.new_tag("td")
    tr2.append(td3)
    td4_style = f"line-height: 1.5; background-color: {row.get('color_code', '#F5F5F5')};"
    td4 = soup.new_tag("td", style=td4_style)
    description_span = soup.new_tag("span", id="description", style=f"line-height: 1.5; font-size: {description_font_size};")
    description_span.string = row["description"]
    td4.append(description_span)
    tr2.append(td4)
    nested_table.append(tr2)

    # Create third row for recommendations
    tr3 = soup.new_tag("tr")
    td5 = soup.new_tag("td")
    tr3.append(td5)
    td6_style = f"line-height: 1.5; background-color:{get_bg_color(row['Subheading'])};"
    td6 = soup.new_tag("td", style=td6_style)
    recommendations_header = soup.new_tag("b", style=f"font-size: {recommendations_font_size};")
    recommendations_header.string = "Recommendations:"
    td6.append(recommendations_header)
    td6.append(soup.new_tag("br"))
    
    # Replace '*' with '<br/>' in the recommendation text and then append it
    recommendations_html_content = row["recommendations"].replace("*", "<br/>")
    recommendations_html = BeautifulSoup(recommendations_html_content, 'html.parser')
    #recommendations_html = BeautifulSoup(row["recommendations"], 'html.parser')
    td6.append(recommendations_html)
    tr3.append(td6)
    nested_table.append(tr3)

    # Create fourth row for additional details
    tr4 = soup.new_tag("tr")
    td7 = soup.new_tag("td")
    tr4.append(td7)
    td8 = soup.new_tag("td", bgcolor="#F5F5F5", style="line-height: 1.5;")
    
    genes_analyzed_label = soup.new_tag("b")
    genes_analyzed_label.string = "Genes Analyzed: "
    genes_value = soup.new_tag("span")
    genes_value.string = str(row["genes_analyzed"])
    td8.append(genes_analyzed_label)
    td8.append(genes_value)
    td8.append(soup.new_tag("br"))

    cli_markers_label = soup.new_tag("b")
    cli_markers_label.string = "Number of Gene Markers Found: "
    cli_value = soup.new_tag("span")
    cli_value.string = str(row["cli_markers"])
    td8.append(cli_markers_label)
    td8.append(cli_value)
    td8.append(soup.new_tag("br"))

    gene_markers_analyzed_label = soup.new_tag("b")
    gene_markers_analyzed_label.string = "Number of Gene Markers Analyzed: "
    gene_analyzed_value = soup.new_tag("span")
    gene_analyzed_value.string = str(row["gene_markers_analyzed"])
    td8.append(gene_markers_analyzed_label)
    td8.append(gene_analyzed_value)
    td8.append(soup.new_tag("br"))

    tr4.append(td8)
    nested_table.append(tr4)

    # Appending the nested table to the main table
    main_tr = soup.new_tag("tr")
    main_td = soup.new_tag("td")
    main_td.append(nested_table)
    main_tr.append(main_td)
    soup.table.append(main_tr)

    # Remove the old placeholder row from the soup to avoid duplication
    placeholder_row = soup.find("tr", id="trait_placeholder")
    if placeholder_row:
        placeholder_row.decompose()
   

    
     # Set page number dynamically. Assuming you have some logic to generate this number.
    page_num = index + 1  # Or any logic you have to get the page number
    footer_div = soup.find("div", id="page_number")
    if footer_div:
        footer_div.string = f"Page {page_count}"
    page_count = page_count + 1
        
     # Ensure Table of Contents remains unchanged
    #toc_div = soup.find("div", id="toc")
    #if toc_div:
    #     toc_div.string = "Table of Contents"

    # Save the updated HTML to a unique file based on the trait/index
    save_trait_to_file(index, soup)


with open(glb_03_output + "\\allout.html", "a", encoding='utf-8') as file:  # added encoding parameter here
    file.write(" ")
    file.write(" ")
    file.write("<div style='page-break-after: always;'></div>")
    file.write(original_disclaimer_template)  

convert_html_to_pdf(glb_03_output + "\\allout.html",glb_03_output + "\\output_stage.pdf")

## Try using Ghostscript for pdf merge
command = [
            ghostscript_bin, 
            "-q", "-dNOPAUSE", "-dBATCH", "-sDEVICE=pdfwrite",
            "-sOutputFile=" + glb_03_output + "\\final_output.pdf",
            glb_template_root + "\\page_1.pdf",glb_03_output + "\\output_stage.pdf",glb_template_root + "\\last_page.pdf"
]
subprocess.run(command, shell=True)  

### Delete residues
delete_files_in_folder(glb_04_temp_input)
#os.remove(glb_03_output + "//output_stage.pdf")
#os.remove(glb_03_output + "//allout.html")


