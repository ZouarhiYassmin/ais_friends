import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.aisfriends.com/vessels/AFRICAN-PUFFIN/9636448/311000789/76417"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

print(soup.title.text)
tables = soup.find_all("table")

print(len(tables))
table = tables[0]
rows = table.find_all("tr")

data = []

for row in rows[1:]:
    cols = row.find_all("td")
    cols = [c.text.strip() for c in cols]
    data.append(cols)

df = pd.DataFrame(data)
print(df.head())
df.to_csv("african_puffin_ais.csv", index=False)