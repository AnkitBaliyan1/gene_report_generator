#import libraries
import os
import sys
import pandas as pd
from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas

# Defining folder path
glb_root = ("04_resources/")
glb_01_input = (glb_root + r"\01_input")
glb_02_master = (glb_root + r"\02_master")
glb_03_output = (glb_root + r"\03_output")

def generate_report(dataframe, output_file):
    # Create a new PDF with Reportlab
    c = canvas.Canvas(output_file, pagesize=landscape(letter))
    width, height = landscape(letter)
    
    # Set up some constants for padding
    top_margin = 50
    left_margin = 50
    row_height = 25
    current_height = height - top_margin
    
    # Title for the report
    c.setFont("Helvetica-Bold", 20)
    c.drawString(left_margin, current_height, "Total Amount Spent by Each Client")
    current_height -= (1.5 * row_height)
    
    # Write the header row
    headers = ["Name", "Amount"]
    col_widths = [300, 300]
    
    c.setFont("Helvetica-Bold", 14)
    for idx, header in enumerate(headers):
        c.drawString(left_margin + sum(col_widths[:idx]), current_height, header)
    
    current_height -= row_height
    
    # Write the data rows
    c.setFont("Helvetica", 12)
    for _, row in dataframe.iterrows():
        for idx, item in enumerate(row):
            c.drawString(left_margin + sum(col_widths[:idx]), current_height, str(item))
        current_height -= row_height
        
    c.save()

# Ensure there are correct command-line arguments
if len(sys.argv) != 3:
    print("Usage: python script_name.py path_to_dummy_client.csv path_to_master.csv")
    sys.exit(1)

# Retrieve file paths from command-line arguments
client_csv_path = sys.argv[1]
master_csv_path = sys.argv[2]


# Load the data from respective paths
client_data = pd.read_csv(client_csv_path)
master_data = pd.read_csv(master_csv_path)

# Merge the dataframes on the 'ID' column
merged_data = pd.merge(client_data, master_data, on='ID')

# Group by the client and sum their transaction amounts
total_spent = merged_data.groupby('Name')['Amount'].sum().reset_index()

# Generate the report and save in the output path
generate_report(total_spent, glb_03_output + r"\client_report.pdf")

print("Report generated in the output path 'client_report.pdf'")


