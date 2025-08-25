import boto3
import time
import csv
import io

# Initialize Kinesis client
kinesis = boto3.client('kinesis', region_name='ap-south-1')
stream_name = 'aahash-newstream'  # Replace with your actual stream name

# Open and read the CSV
with open('viewership_logs.csv', 'r') as file:
    reader = csv.reader(file)
    headers = next(reader)  # Read header row and skip

    for row in reader:
        # Convert the row back into a single CSV string
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(row)
        csv_data = output.getvalue().strip()

        # Send to Kinesis
        response = kinesis.put_record(
            StreamName=stream_name,
            Data=csv_data.encode('utf-8'),
            PartitionKey=row[0] if row[0] else 'default'  # Use session_id or fallback
        )

        print(f"✅ Sent CSV row: {csv_data} | Response: {response}")
        time.sleep(0.5)  # Optional: simulate streaming by throttling
