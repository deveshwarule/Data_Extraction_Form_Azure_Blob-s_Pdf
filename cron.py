from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from app import process_pdf_files

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
scheduler.start()

# Function to trigger PDF processing
def trigger_pdf_processing():
    try:
        pdf_files = process_pdf_files()
        if len(pdf_files) > 0:
            print("Data inserted successfully.")
        else:
            print("All PDFs data are already extracted.")
    except Exception as e:
        print(f"Error processing PDFs: {str(e)}")

job = scheduler.add_job(trigger_pdf_processing, 'interval', minutes=15)

# Run the Flask app
if __name__ == '__main__':
    trigger_pdf_processing()
    while True:
        try:
            app.run()
        except Exception as e:
            print(f"Error running Flask app: {str(e)}")
