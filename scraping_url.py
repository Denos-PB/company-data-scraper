import asyncio
import requests
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

api_semaphore = asyncio.Semaphore(50)

def create_session():
    """
    Creates and configures a global Requests session for high-concurrency API calls.

    Configuration includes:
    - Automatic retries 3 times.
    - Connection pooling to reuse TCP sockets and improve speed.
    
    Returns:
        requests.Session: A configured session object.
    """
    session = requests.Session()
    retry = Retry(connect=2, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

global_session = create_session()

def clean_name(name):
    """
    Normalizes a company name string to facilitate unclear matching.
    
    Logic:
    - Converts to lowercase.
    - Removes common legal suffixes (Inc, LLC, Ltd, Corp, etc.).
    - Removes all non-alphanumeric characters.
    
    Args:
        name (str):Raw company name.

    Returns:
        str: Cleaned core name.
    """
    if not name: return ""
    name = name.lower()
    name = re.sub(r'\b(inc|llc|ltd|corp|limited|company|co)\b', '', name)
    name = re.sub(r'[^a-z0-9]', '', name)
    return name

def _sync_clearbit(company_name):
    """
    Synchronous function that queries the Clearbit API for a company website.

    Logic:
    - Fetches the top 3 suggestions from the API.
    - Compares the cleaned input name against the API's returned domain and name.
    - Accepts the result only if there is a partial string match.
    
    Args:
        company_name (str): The name of the company to find.

    Returns:
        str | None: The found website URL (e.g., "https://apple.com") or None.
    """
    url = "https://autocomplete.clearbit.com/v1/companies/suggest"
    try:
        response = global_session.get(url = url, params ={'query':company_name}, headers=HEADERS, timeout=3)
        if response.status_code == 200:
            data = response.json()
            if data:
                clean_query = clean_name(company_name)

                for item in data[:3]:
                    domain = item.get('domain','')
                    item_name = clean_name(item.get('name' , ''))

                    if clean_query in domain.replace('.', ''):
                        print(f"API Match: {company_name} -> {domain}")
                        return f"https://{domain}"
                    
                    if clean_query in item_name or item_name in clean_query:
                         print(f"API Name Match: {company_name} -> {domain}")
                         return f"https://{domain}"
    except:
        pass
    
    return None

async def fetch_api(company_name):
    """
    Asynchronous wrapper for the synchronous API worker.
    
    Uses a semaphore to throttle requests and runs the 
    blocking request in a separate thread.

    Args:
        company_name (str): Name of the company.

    Returns:
        str | None: Founded URL.
    """
    async with api_semaphore:
        return await asyncio.to_thread(_sync_clearbit, company_name)

async def get_urls_api(company_list):
    """
    Main entry point to process a batch of company names concurrently.

    Args:
        company_list (list): A list of company name strings.

    Returns:
        list[str | None]: A list of URLs corresponding to the input list order.
    """
    total = len(company_list)
    
    results: list[str | None] = [None] * total

    tasks = [fetch_api(name) for name in company_list]
    api_results = await asyncio.gather(*tasks)

    for i, url in enumerate(api_results):
        if url:
            results[i] = str(url)

    return results