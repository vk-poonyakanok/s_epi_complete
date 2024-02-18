from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Start a new instance of Chrome
driver = webdriver.Chrome()

# Open the webpage
driver.get('https://healthgate.hss.moph.go.th/search-hospital')

# Wait for the dropdown to be loaded
wait = WebDriverWait(driver, 10)
element = wait.until(EC.presence_of_element_located((By.ID, 'search_province')))

# Select a province from the dropdown
select_province = Select(driver.find_element(By.ID, 'search_province'))
select_province.select_by_visible_text('กรุงเทพมหานคร') # Change this to the province you're interested in

# Submit the form directly after selecting the province
form = driver.find_element(By.TAG_NAME, 'form')  # This finds the first <form> element on the page
form.submit()

# Wait for the search results to load
# You may need to adjust this to wait for a specific element that indicates the page has loaded
wait.until(EC.presence_of_element_located((By.ID, 'some_result_element_id')))  # Update 'some_result_element_id' to a real ID that appears after search

# Example to find table rows. Adjust the selector as needed based on the actual table structure
table_rows = driver.find_elements(By.XPATH, '//table//tr')  # Adjust XPATH based on the actual table structure

for row in table_rows:
    # Extract each cell in the row
    cells = row.find_elements(By.TAG_NAME, 'td')
    row_data = [cell.text for cell in cells]
    print(row_data)  # or process/store the data as needed

# Here, add the code to submit the form. This might involve clicking a search button, for example:
# search_button = driver.find_element(By.ID, 'search_button_id_here')
# search_button.click()

# Wait for the search results to load and process them
# For example, to print the page source: print(driver.page_source)

# Close the browser
#driver.quit()