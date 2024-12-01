import requests
from bs4 import BeautifulSoup

cik = '0000320193'  # Example CIK for Apple Inc.
url = f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}&action=getcompany"

headers = {
    "User-Agent": "toronto.taolin@gmail.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
    "Connection": "keep-alive",
}

response = requests.get(url, headers = headers)
# Check for success
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    # Extract exchange information
    header_info = soup.find('span', class_='company-header')
    if header_info:
        print(header_info.text.strip())
    else:
        print("Exchange information not found.")
else:
    print(f"Error: {response.status_code} - {response.reason}")
# soup = BeautifulSoup(response.text, 'html.parser')

# # Find exchange information in the company header
# header_info = soup.find('span', class_='company-header')
# print(header_info.text.strip())