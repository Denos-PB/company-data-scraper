import asyncio
import pandas as pd
import requests
import re
import tldextract
import dns.resolver
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-z0-9.-]+\.[a-zA-Z]{2,}'

BAD_EXTENSIONS = ('.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.svg', '.webp')

TARGET_KEYWORDS = ['contact','contact us','support', 'about','about us', 'connect', 'reach', 'help']

email_semaphore = asyncio.Semaphore(10)

def check_mx_records(email):
    """
    Validates if an email address is functional by checking the DNS Mail Exchange records.

    Args:
        email (str): Email address.

    Returns:
        bool: True if the domain has valid MX records, False otherwise.
    """
    try:
        domain = email.split('@')[1]
        records = dns.resolver.resolve(domain, 'MX')
        return bool(records)
    except:
        return False
    
def extract_valid_emails(html,current_url):
    """
    Parses HTML content to find, clean, and filter email addresses associated with the company's domain.

    Logic:
    - This function performs de-obfuscation.
    - Filters out files that resemble emails.
    - Strictly checks that the email domain matches the website's core name.

    Args:
        html (str): The raw HTML.
        current_url (str): The URL of the page.

    Returns:
        list: A list of unique, valid emails.
    """
    found = set()

    html = html.replace(' [at] ', '@').replace('(at)', '@').replace(' at ', '@')
    raw_emails = re.findall(EMAIL_REGEX, html)

    site_info = tldextract.extract(current_url)
    site_name = site_info.domain.lower()

    for email in raw_emails:
        email = email.lower()

        if email.endswith(BAD_EXTENSIONS): continue
        if len(email) > 50: continue

        email_domains = email.split('@')[1]

        if site_name in email_domains:
            found.add(email)
    
    return list(found)

def _scrape_sync_email(website_url):
    """
    The main synchronous function that orchestrates the scraping strategy for a single website.

    Logic:
    - Scans the Homepage for emails.
    - If none found, hunts for any matches in list of TARGET_KEYWORDS pages and scans them.
    - Verifies the found candidate using DNS MX records.

    Args:
        website_url (str): The URL of the company website.

    Returns:
        str | None: A verified email address, otherwise None.
    """
    if not website_url or pd.isna(website_url):
        return None
    
    session = requests.Session()

    try:
        try:
            resp = session.get(url=website_url, headers=HEADERS, timeout=10, verify=False)
            html = resp.text
            final_url = resp.url
        except:
            return None
        
        emails = extract_valid_emails(html, final_url)

        if not emails:
            soup = BeautifulSoup(html, 'html.parser')
            target_link = None
            

            for a in soup.find_all('a', href=True):
                text = str(a.text).lower()
                href = str(a['href']).lower()

                if any(key in text for key in TARGET_KEYWORDS) or any(key in href for key in TARGET_KEYWORDS):
                    target_link = urljoin(final_url, str(a['href']))

                    if 'contact' in text or 'contact' in href:
                        break

            if target_link:
                try:
                    resp_contact = session.get(url=target_link, headers=HEADERS, timeout=12, verify=False)
                    emails = extract_valid_emails(resp_contact.text, final_url)
                except:
                    pass

        if emails:
            candidate = emails[0]

            if check_mx_records(candidate):
                return candidate
            else:
                print(f"Skipped {candidate} (Bad MX)") 
                return None
            
    except:
        pass

    return None

async def find_email_async(website_url):
    """
    Asynchronous wrapper for the synchronous scraping function.

    Uses a semaphore to limit concurrent network requests and runs the 
    blocking scraping logic in a separate thread.

    Args:
        website_url (str): URL to process.

    Returns:
        str | None: result (email or None).
    """
    
    async with email_semaphore:
        email = await asyncio.to_thread(_scrape_sync_email, website_url)

        if email:
            print(f"Found and Verified: {email}")
        else:
            print(f"No email for {website_url}")

        return email