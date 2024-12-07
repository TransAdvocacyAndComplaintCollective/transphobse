import re
import pandas as pd

text = """
Complaints to the BBC  

This fortnightly report for the BBC complaints service1 shows for the periods covered :

 the number of complaints about programmes and those which received more 
than 1002 at Stage 1 (Audience Services);  
 findings of subsequent investigations made at Stage 2 (by the Executive 
Complaints Unit)3; 
 the percentage of all complaints dealt with within the target periods for each 
stage.

NB: Figures include, but are not limited to, editorial complaints, and are not comparable with 
complaint figures published by Ofcom about other broadcasters (which are calculated on a 
different basis). The number of complaints received is not an indication of how serious an issue is.  

Stage 1 complaints  

Between 27 May - 9 June 2019, BBC Audience Services (Stage 1) received a total of  
6,484 complaints about programmes. 9,812 complaints in total were received at Stage 1.  
BBC programmes which received more than 1002 complaints are included in the table 
below:   

Programme  Service  Date of 
Transmission   Main Issue(s)  Number of 
Complaints  
Victoria 
Derbyshire  BBC Two  09/05/2019 Balloon image of President 
Trump in the studio was 
inappropriate /biased 
against him.  991 
BBC News 
Special: EU 
Elections 2019  BBC One  26/05/2019 Claims of bias both against 
and in favour of Brexit or 
the Brexit Party.  

Alastair Campbell implying  
the Brexit Party are funded 
by Russia.   573

Today  Radio 4  07/06/2019  Bias against Andy 
McDonald and/or 
misrepresenting President 
Trump’s view on the NHS 
and any future US/UK trade 
deal.  273

90% of all complaints dealt with between 27 May - 9 June 2019 received an initial 
response within the stage 1 target period of 10 working days.

Recent BBC public responses to significant complaints at Stage 1 are published at:
http://www.bbc.co.uk/complaints/complaint/

Stage 2 complaints – Executive Complaints Unit (ECU)  

The Executive Complaints Unit made 11 findings at Stage 2 between 27 May - 9 June 
2019. Further information on complaints which were upheld or resolved after 
investigation by the ECU can be found here: http://www.bbc.co.uk/complaints/comp-
reports/ecu/   

Programme  Service  Date of 
Transmission  Issue  Outcome  
MotherFatherSon  BBC Two  06/03/2019  Sex and strong 
language  x2 Not 
Upheld  
Climate Change: 
The Facts  BBC One  18/04/2019  Inaccurate 
reporting of 
climate change  Not 
Upheld  
BBC News at Ten  BBC One  23/01/2019  Inaccurate 
statement that 
Martin Lewis 
doesn’t do 
advertisements  Not 
Upheld  
Phil Upton  BBC Coventry 
and 
Warwickshire  02/04/2019  False claim to have 
originated story 
about Coventry 
City  Not 
Upheld  
Countryfile  BBC One  07/04/2019  Failure to credit 
Gerald 
Hawthornthwaite’s  
role in creation of 
first national park  Not 
Upheld  

The World Tonight  Radio 4  01/04/2019  Pro-Remain bias in 
report of Commons 
indicative votes  Not 
Upheld  
BBC Six O’Clock 
News & BBC News 
at Ten  BBC One  11/04/2019  Failed to state 
party affiliation of 
interviewee  Upheld  
Business Briefing  BBC News 
Channel  28/03/2019  Appearance of bias 
against no-deal 
Brexit x2  Upheld  
Comic Relief  BBC One  15/03/2019  Scenes of male 
strippers 
objectified men  Not 
Upheld  

91% of complaints (10 out of 11) dealt with between 27 May - 9 June 2019 received a 
response within the target time.
"""

# Extract Stage 1 complaints
stage1_pattern = re.compile(
    r"Stage 1 complaints\s*(.*?)\s*Stage 2 complaints", re.S
)
stage1_data = stage1_pattern.search(text)
if stage1_data:
    stage1_text = stage1_data.group(1)
else:
    stage1_text = ""
    print("Stage 1 data not found.")

# Extract individual complaints for Stage 1
stage1_entries = stage1_text.strip().split('\n\n')

stage1_complaints = []
for entry in stage1_entries:
    # Clean up entry
    entry = ' '.join(entry.strip().split())
    # Skip if entry is empty or does not contain data
    if not entry or 'Programme' in entry:
        continue
    # Extract data fields
    match = re.match(
        r'(.*?)\s{2,}(.*?)\s{2,}(.*?)\s{2,}(.*)\s+(\d+)$', entry
    )
    if match:
        stage1_complaints.append(match.groups())
    else:
        # Handle entries that span multiple lines
        parts = entry.split()
        if parts:
            # Assume last part is number of complaints
            number_of_complaints = parts[-1]
            # Remaining parts
            remaining = ' '.join(parts[:-1])
            # Try to split remaining into fields
            fields = re.split(r'\s{2,}', remaining)
            if len(fields) >= 4:
                stage1_complaints.append((
                    fields[0],
                    fields[1],
                    fields[2],
                    fields[3],
                    number_of_complaints
                ))
            else:
                # If still not enough fields, skip or handle as needed
                continue

# Create a DataFrame for Stage 1
stage1_columns = ["Programme", "Service", "Date of Transmission", "Main Issue(s)", "Number of Complaints"]
stage1_df = pd.DataFrame(stage1_complaints, columns=stage1_columns)

# Extract Stage 2 complaints
stage2_pattern = re.compile(
    r"Stage 2 complaints.*?\n\n(.*?)\n\n\d+% of complaints", re.S
)
stage2_data = stage2_pattern.search(text)
if stage2_data:
    stage2_text = stage2_data.group(1)
else:
    stage2_text = ""
    print("Stage 2 data not found.")

# Extract individual complaints for Stage 2
stage2_entries = stage2_text.strip().split('\n\n')

stage2_complaints = []
for entry in stage2_entries:
    # Clean up entry
    entry = ' '.join(entry.strip().split())
    # Skip if entry is empty or does not contain data
    if not entry or 'Programme' in entry:
        continue
    # Extract data fields
    match = re.match(
        r'(.*?)\s{2,}(.*?)\s{2,}(.*?)\s{2,}(.*)\s{2,}(.*)$', entry
    )
    if match:
        stage2_complaints.append(match.groups())
    else:
        # Handle entries that span multiple lines
        parts = entry.split()
        if parts:
            # Try to identify Outcome (e.g., 'Upheld' or 'Not Upheld')
            outcome = parts[-2] + ' ' + parts[-1]
            # Remove outcome from parts
            parts = parts[:-2]
            # Remaining parts
            remaining = ' '.join(parts)
            # Try to split remaining into fields
            fields = re.split(r'\s{2,}', remaining)
            if len(fields) >= 4:
                stage2_complaints.append((
                    fields[0],
                    fields[1],
                    fields[2],
                    fields[3],
                    outcome
                ))
            else:
                # If still not enough fields, skip or handle as needed
                continue

# Create a DataFrame for Stage 2
stage2_columns = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome"]
stage2_df = pd.DataFrame(stage2_complaints, columns=stage2_columns)

# Display the DataFrames
print("Stage 1 Complaints:")
print(stage1_df)

print("\nStage 2 Complaints:")
print(stage2_df)
