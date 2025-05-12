import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import csv
import os, sys
from example_spark import analyse_with_pyspark

os.environ['PYSPARK_PYTHON'] = sys.executable


class FlatScraper:
    PRICE_THRESH = 1000
    MIN_SPACE = 15

    def scrap_olx(self):
        url = f'https://www.olx.pl/nieruchomosci/mieszkania/gdansk/'

        url_to_add = 'https://www.olx.pl'

        headers = {
            'User-Agent': 'Chrome/136.0.0.0'
        }

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        # pages count
        x = int(soup.find_all('a', class_='css-b6tdh7')[-1].text.strip())
        offers = []
        for i in range(1, x):
        # for i in range(1, 2):

            url = f'https://www.olx.pl/nieruchomosci/mieszkania/gdansk/?page={i}'

            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            offers += soup.find_all('div', class_='css-1apmciz')

        data = []
        for offer in offers:
            try:
                title = offer.find('h4').text.strip()
                price = offer.find('p', class_='css-uj7mm0').text.strip()
                location = offer.find('p', class_='css-vbz67q').text.strip()
                loc_date_delimiter = location.find('-')
                date = location[loc_date_delimiter+1:]
                location = self._normalize_location(location[:loc_date_delimiter-1])
                space = offer.find('span', class_='css-6as4g5').text.strip()
                hl = re.search(r'href="([^"]+)"', str(offer.find('a', class_='css-1tqlkj0'))).group(1)

                if '-' not in space:
                    space_num = float(re.findall(r'\d+', space)[0])
                else:
                    space_num = float('.'.join(re.findall(r'\d+', space[:space.find('-')-1])))

                if hl.startswith('/d/'):

                    hl = url_to_add + hl

                price_clean = price.replace(' ', '').replace(',', '.').replace('zł', '').replace('donegocjacji', '')
                price_num = float(price_clean)
                if price_num/space_num > self.PRICE_THRESH and space_num > self.MIN_SPACE:
                    data_entry = {
                        'Location': location,
                        'Price': price_num,
                        'm2': space_num,
                        'p/m2': price_num/space_num,
                        'Date': date,
                        'Text': title,
                        'hl': hl
                    }
                    if data_entry not in data:
                        data.append(data_entry)

            except Exception as e:
                print(f'Error: {e}')

        return data

    def scrap_ot(self):
        url = f'https://ogloszenia.trojmiasto.pl/nieruchomosci/mieszkanie/gdansk/'

        headers = {
            'User-Agent': 'Chrome/136.0.0.0'
        }

        offers = []
        data = []

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        page = 1

        def check_if_next_page(page, soup):
            pages = soup.find_all('a', class_='pages__item__link')
            
            if str(page + 1) in [x.text.strip() for x in pages]:
                return True
            else:
                return False

        while check_if_next_page(page, soup):
            page += 1
            url = f'https://ogloszenia.trojmiasto.pl/nieruchomosci/mieszkanie/gdansk/?strona={page-1}'
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            offers += soup.find_all('div', class_='list__item__wrap__content')

        for offer in offers:
            try:
                title = offer.find('h2').text.strip()
                price = offer.find('div', class_='list__item__price').text.strip()
                location = self._normalize_location(offer.find('p', class_='list__item__content__subtitle').text.strip())
                date = offer.find('div', class_='listItemFooter__date').find('span').text.strip()
                space = float(offer.find('p', class_='list__item__details__icons__element__desc').text.strip()[:-2])
                hl = re.search(r'href="([^"]+)"', str(offer.find('a', class_='list__item__content__title__name link'))).group(1)

                price_clean = price.replace(' ', '').replace(',', '.').replace('zł', '').replace('donegocjacji', '')
                price_num = self._handle_ot_price(price_clean)

                if price_num/space > self.PRICE_THRESH and space > self.MIN_SPACE:
                    data_entry = {
                        'Location': location,
                        'Price': float(price_num),
                        'm2': space,
                        'p/m2': float(price_num/space),
                        'Date': date,
                        'Text': title,
                        'hl': hl
                    }
                    if data_entry not in data:
                        data.append(data_entry)

            except Exception as e:
                print(f'Offer error: {e}')

        return data

    def _handle_ot_price(self, price):
        if price == 'Zapytajocenę':
            return None
        if 'Nowacena' in price:
            counter = 0
            for i in price[::-1]:
                if not i.isdigit() and not i == ',':
                    return float(price[-counter:])
                else:
                    counter += 1
        return float(price)
    
    def _normalize_location(self, location):
        location = re.sub(r'\s+', ' ', location).strip()
        if len(location)> 6:
            if location[6] != ',' and ',' in location:
                ind = location.index(',')
                return location[:6] + ',' + location[6:ind]
            elif location[6] != ',':
                return location[:6] + ',' + location[6:]
        else:
            return location + ' (region not specified)'
        return location

    def save_to_csv(self, data, name):
        try:
            sorted_data = sorted(data, key=lambda x: x['p/m2'])

            df = pd.DataFrame(sorted_data)
            df.to_csv(f'{name}.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
            print(f'Data saved in {name}.csv')
        except Exception as e:
            print(f'Error saving to csv: {e}')

    def base_scrapper(self, filename='data', olx=True, ot=True):
        data = []
        if olx:
            data.extend(self.scrap_olx())
        if ot:
            data.extend(self.scrap_ot())
        analyse_with_pyspark(data, filename)
        # self.save_to_csv(data, filename)


if __name__ == '__main__':
    scrp = FlatScraper()
    scrp.base_scrapper()