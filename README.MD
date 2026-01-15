# Companies General Data Collection

## Project Description

This project automates the collection of general data for companies, with a particular focus on those specialising in women's health. It processes a list of company names, retrieves their official websites using the Clearbit API, and then extracts verified email addresses from those websites. The resulting comprehensive CSV file contains company names, websites, and associated emails.
The tool is designed for efficiency with asynchronous operations, allowing concurrent API calls and web scraping to handle large datasets quickly.



## Installation Instructions

1. Ensure you have Python 3.9 or higher installed on your system.
2. Clone or download this repository to your local machine.
3. Navigate to the project directory.
4. Download uv: 
    On macOS and Linux
    ```
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
    On Windows.
    ```
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```
    Or, from PyPI:

    With pip.
    ```
    pip install uv
    ```
    Or pipx.
    ```
    pipx install uv
    ```
5. Create a virtual environment:
   ```
   uv venv
   ```
6. Activate the virtual environment(if not activate it by uv):
   - On Windows: `.venv\Scripts\activate`
   - On macOS/Linux: `source .venv/bin/activate`
7. Install the required dependencies:
   ```
   uv pip install -r requirements.txt
   ```

## Usage Examples

1. Prepare your input CSV file with company names. The file should have a column named "Company Name" containing the list of companies to process. Place this file in the `unstracted_data/` directory (e.g., `unstracted_data/Table on Test Task - Sheet1.csv`).

2. Run the main script:
   ```
   python main.py
   ```

3. The script will process the companies asynchronously:
   - Fetch website URLs using the Clearbit API.
   - Scrape and verify email addresses from the retrieved websites.
   - Output the results to `data/companies_complete.csv`.

Example output CSV structure:
```
Company Name,Website,Email
Company A,https://www.companya.com,contact@companya.com
Company B,https://www.companyb.com,info@companyb.com
```

## Features

- Asynchronous processing for high-performance data collection
- Integration with Clearbit API for accurate website discovery
- Web scraping with email validation using DNS MX records
- Robust error handling and logging
- Support for large datasets with rate limiting and semaphores
- Clean data normalization for company name matching

## License Information

This project is licensed under the MIT License. See the LICENSE file for details.