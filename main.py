import asyncio
from scraping_url import get_urls_api
from get_email import find_email_async
import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

OUTPUT_FILE = "data/companies_complete.csv"

async def main():
    try:
        df = pd.read_csv("unstracted_data/Table on Test Task - Sheet1.csv")
        names = df['Company Name'].tolist()
        
        urls = await get_urls_api(names)
        df['Website'] = urls

        email_tasks = [find_email_async(url) for url in df['Website']]
        emails = await asyncio.gather(*email_tasks)

        df['Email'] = emails
        
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"Saved to {OUTPUT_FILE}")

    except FileNotFoundError:
        print("Error: CSV file not found.")
    except Exception as e:
        print(f"Critical Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())