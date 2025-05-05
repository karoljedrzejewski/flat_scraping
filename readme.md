# Gdańsk Flat Scraper 🏠

A Python script for scraping apartment listings from **OLX.pl** and **ogloszenia.trojmiasto.pl** (category: real estate → apartments → Gdańsk). The data is filtered, processed, and saved to a CSV file.

## 📦 What does the script do?

- Navigates through all pages of apartment listings in Gdańsk on OLX.pl
- Extracts key information such as:
  - Location
  - Price
  - Area in m²
  - Price per m²
  - Listing date
  - Title
  - Link to the listing
- Filters out listings:
  - smaller than 30 m²
  - or with price per m² lower than `PRICE_THRESH` (default: 2000 PLN/m²)
- Saves the cleaned data to a `data.csv` file with properly quoted text fields

## 📄 Sample CSV output

```csv
"Location","Price","m2","p/m2","Date","Text","hl"
"Gdańsk, Zaspa","490000",56.0,8750.0,"28 April 2025","3-room flat with balcony","https://www.olx.pl/d/oferta/..."
