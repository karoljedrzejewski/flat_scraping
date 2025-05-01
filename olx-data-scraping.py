import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import csv

url = f"https://www.olx.pl/nieruchomosci/mieszkania/gdansk/"

url_to_add = 'https://www.olx.pl'

PRICE_THRESH = 2000

headers = {
    "User-Agent": "Chrome/136.0.0.0"
}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

# pages count
x = int(soup.find_all("a", class_="css-b6tdh7")[-1].text.strip())
offers = []
for i in range(1, x):

    url = f"https://www.olx.pl/nieruchomosci/mieszkania/gdansk/?page={i}"

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    offers += soup.find_all("div", class_="css-1apmciz")

data = []
for offer in offers:
    try:
        title = offer.find("h4").text.strip()
        price = offer.find("p", class_="css-uj7mm0").text.strip()
        location = offer.find("p", class_="css-vbz67q").text.strip()
        loc_date_delimiter = location.find("-")
        date = location[loc_date_delimiter+1:]
        location = location[:loc_date_delimiter-1]
        space = offer.find("span", class_="css-6as4g5").text.strip()
        hl = re.search(r'href="([^"]+)"', str(offer.find("a", class_="css-1tqlkj0"))).group(1)

        if "-" not in space:
            space_num = float(re.findall(r"\d+", space)[0])
        else:
            space_num = float('.'.join(re.findall(r"\d+", space[:space.find("-")-1])))

        if hl.startswith('/d/'):
            hl = url_to_add + hl

        price_clean = price.replace(" ", "").replace(",", ".").replace("zł", "").replace("donegocjacji", "")
        price_num = round(float(price_clean))
        if price_num/space_num > PRICE_THRESH and space_num > 30:
            data_entry = {
                "Location": location,
                "Price": price_num,
                "m2": space_num,
                "p/m2": price_num/space_num,
                "Date": date,
                "Text": title,
                "hl": hl
            }
            if data_entry not in data:
                data.append(data_entry)

    except Exception as e:
        print(f"Error: {e}")

sorted_data = sorted(data, key=lambda x: x["p/m2"])

df = pd.DataFrame(sorted_data)
df.to_csv("flat_olx.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
print("Data saved in flat_olx.csv")