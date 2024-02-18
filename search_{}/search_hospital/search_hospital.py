from urllib.parse import quote
from gazpacho import get, Soup
import pandas as pd

# Fetch the main page HTML
main_url = "https://healthgate.hss.moph.go.th/search-hospital"
main_html = get(main_url)
main_soup = Soup(main_html)

# Find the <select> element for provinces
select_element = main_soup.find('select', {'id': 'search_province'})

# Convert the select_element back to HTML string if necessary or directly work with it
select_html = str(select_element)

# `soup` is the parsed HTML of the main page where provinces are listed
select = Soup(select_html)  # Parse the select HTML segment containing provinces
options = select.find('option', mode='all')[1:]  # Skip the first option which is usually a placeholder

# List to hold data from all provinces
all_data = []

for option in options:
    province_name = option.text
    province_value = quote(province_name)  # Ensure the province name is URL-encoded

    # Dynamically build the URL for each province
    url = f"https://healthgate.hss.moph.go.th/search/hospital?search_province={province_value}"
    
    # Fetch the HTML content for each province
    html = get(url)
    soup = Soup(html)

    # Repeat the extraction process for each province
    table_body = soup.find('tbody')
    rows = table_body.find('tr', mode='all')

    for row in rows:
        cells = row.find('td', mode='all')
        row_data = [cell.text for cell in cells]
        all_data.append(row_data)

# Convert all collected data into a DataFrame
df = pd.DataFrame(all_data, columns=["จังหวัด", "อำเภอ", "ตำบล", "ชื่อหน่วยงาน/รพสต.", "Login Code"])

# Export the DataFrame to a CSV file
df.to_csv('search_hospital.csv', index=False)