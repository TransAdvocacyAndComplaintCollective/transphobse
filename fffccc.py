import re
import pandas as pd

# Step 1: Store the text in a multi-line string
text = """
[Your text here]
"""

# Function to extract table data
def extract_table_data(text, start_pattern, end_pattern):
    # Extract the table text between the start and end patterns
    table_text_match = re.search(f"{start_pattern}(.*?){end_pattern}", text, re.DOTALL)
    if not table_text_match:
        return None
    table_text = table_text_match.group(1)
    
    # Split the table text into lines and remove empty lines
    lines = [line.strip() for line in table_text.strip().split('\n') if line.strip()]
    
    # Group lines into blocks separated by empty lines (each block corresponds to one complaint)
    blocks = []
    current_block = []
    for line in lines:
        if not line.strip():
            if current_block:
                blocks.append(current_block)
                current_block = []
        else:
            current_block.append(line)
    if current_block:
        blocks.append(current_block)
    
    # Parse each block to extract data
    data = []
    for block in blocks:
        # Combine the lines in the block into one string
        block_text = ' '.join(block)
        
        # Extract 'Number of Complaints' (assumed to be the last number in the block)
        number_match = re.search(r'(\d+)$', block_text)
        if number_match:
            number_of_complaints = number_match.group(1)
            block_text = block_text[:number_match.start()].strip()
        else:
            number_of_complaints = ''
        
        # Extract Programme, Service, Date of Transmission, and Main Issue(s)
        # Split by two or more spaces
        parts = re.split(r'\s{2,}', block_text)
        
        if len(parts) >= 4:
            programme = parts[0]
            service = parts[1]
            date_of_transmission = parts[2]
            main_issues = ' '.join(parts[3:])
        else:
            # Handle cases where splitting didn't work
            programme = parts[0] if len(parts) > 0 else ''
            service = parts[1] if len(parts) > 1 else ''
            date_of_transmission = parts[2] if len(parts) > 2 else ''
            main_issues = ' '.join(parts[3:]) if len(parts) > 3 else ''
        
        data.append({
            'Programme': programme,
            'Service': service,
            'Date of Transmission': date_of_transmission,
            'Main Issue(s)': main_issues,
            'Number of Complaints': number_of_complaints
        })
    return data

# Step 3: Extract Stage 1 complaints data
start_pattern_stage1 = r"Programme\s+Service\s+Date of\s+Transmission\s+Main Issue\(s\)\s+Number of\s+Complaints"
end_pattern_stage1 = r"\d+% of all complaints dealt with"

stage1_data = extract_table_data(text, start_pattern_stage1, end_pattern_stage1)

# Convert to DataFrame
if stage1_data:
    df_stage1 = pd.DataFrame(stage1_data)
    print("Stage 1 Complaints Data:")
    print(df_stage1)
else:
    print("Stage 1 data not found.")

# Step 4: Extract Stage 2 complaints data
start_pattern_stage2 = r"Programme\s+Service\s+Date of\s+Transmission\s+Issue\s+Outcome"
end_pattern_stage2 = r"\d+% of complaints"

stage2_data = extract_table_data(text, start_pattern_stage2, end_pattern_stage2)

# Convert to DataFrame
if stage2_data:
    df_stage2 = pd.DataFrame(stage2_data)
    print("\nStage 2 Complaints Data:")
    print(df_stage2)
else:
    print("Stage 2 data not found.")
