import pandas as pd
import matplotlib.pyplot as plt

# Load the data from a CSV file
csv_file_path = "/home/lucy/Desktop/transphobse/data/bbc_transphobia_complaint_service_reports_keywords_matched.csv"
df = pd.read_csv(csv_file_path)

# Convert Date of Transmission to datetime
df['Date of Transmission'] = pd.to_datetime(df['Date of Transmission'], format='%d/%m/%Y', errors='coerce')

# Drop rows where the date could not be parsed
df = df.dropna(subset=['Date of Transmission'])

# Group by month and count the number of complaints per month
df['YearMonth'] = df['Date of Transmission'].dt.to_period('M')  # Grouping by year and month
monthly_complaints = df.groupby('YearMonth').size()

# Plotting the trend over time
plt.figure(figsize=(10, 6))
monthly_complaints.plot(kind='line', marker='o', color='skyblue')
plt.title('Complaints Over Time')
plt.xlabel('Month')
plt.ylabel('Number of Complaints')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()

# Display plot
plt.show()
