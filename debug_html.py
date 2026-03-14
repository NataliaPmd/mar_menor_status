# Debug: shows the raw text structure of the monitoring HTML page
import requests
from bs4 import BeautifulSoup

url = "https://canalmarmenor.carm.es/monitorizacion/monitorizacion-de-parametros/"
resp = requests.get(url, timeout=30)
soup = BeautifulSoup(resp.text, "lxml")

# Print cleaned text of the full page, line by line (skip blanks)
print("=== PAGE TEXT (line by line) ===")
for line in soup.get_text(separator="\n").splitlines():
    line = line.strip()
    if line:
        print(repr(line))
