from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
import time
from datetime import datetime
import psycopg2
import csv
from selenium.webdriver.common.action_chains import ActionChains
class GlobalDatabaseScraper:
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.driver = webdriver.Chrome()

    def login(self):
        self.driver.get('https://platform.globaldatabase.com/')
        email_field = self.driver.find_element("xpath", '//input[@placeholder="Email address"]')
        password_field = self.driver.find_element("xpath", '//input[@placeholder="Password"]')
        email_field.send_keys(self.email)
        password_field.send_keys(self.password)
        password_field.send_keys(Keys.RETURN)

    def go_to_companies_page(self):
        companies_button = self.driver.find_element("xpath", '//a[@href="/app-aggregator/prospect/companies"]')
        companies_button.click()

    def filter_by_location(self):
        location_filter = self.driver.find_element("xpath", '//span[text()="Location"]')
        location_filter.click()

    def filter_by_country(self, country):
        countries_filter = self.driver.find_element("xpath", '//div[@class="filter-child-title"][text()="Countries"]')
        countries_filter.click()
        search_box = self.driver.find_element("xpath", '//input[@placeholder="Search country"]')
        search_box.send_keys(country)
        time.sleep(5)
        checkbox = self.driver.find_element("xpath", '//span[@class="ant-tree-checkbox-inner"]')
        checkbox.click()


    def scroll(self):
        try:
            body = self.driver.find_element('tag name', 'body')
            loop = 0
            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")
            print("Current Time =", current_time)
            while True:
                loop += 1
                body.send_keys(Keys.DOWN)
                if loop == 100:
                    print('loop for 100')
                    now = datetime.now()

                    current_time = now.strftime("%H:%M:%S")
                    print("Current Time =", current_time)
                if loop == 300:
                    print('loop for 300')
                    now = datetime.now()

                    current_time = now.strftime("%H:%M:%S")
                    print("Current Time =", current_time)
                if loop == 500:
                    print('loop for 500')
                    now = datetime.now()

                    current_time = now.strftime("%H:%M:%S")
                    print("Current Time =", current_time)
                if loop >= 1000:
                    print("---1000 loops---")
                    now = datetime.now()

                    current_time = now.strftime("%H:%M:%S")
                    print("Current Time =", current_time)
                    table = self.driver.find_element("xpath", '//table/tbody')
                    rows = table.find_elements("tag name", "tr")
                    print(len(rows))
                    loop = 0
                    self.data_processing()
        except Exception as e:
            print(e)

        table = self.driver.find_element("xpath", '//table/tbody')
        rows = table.find_elements("tag name", "tr")
        print(len(rows))

    def data_processing(self):
        table = self.driver.find_element("xpath", '//table/tbody')
        rows = table.find_elements("tag name", "tr")
        data = []

        # Add column names to data
        data.append([
            "Company Name",
            "Country",
            "Status",
            "Website",
            "VAT Number",
            "SIC Code",
            "SIC Desc",
            "Industry",
            "Legal Form",
            "Reg No",
            "Address",
            "Alexa Rank",
            "Monthly Visits",
            "Employees",
            "Turnover",
            "Age"
        ])

        for row in rows:
            cols = row.find_elements("tag name", "td")
            time.sleep(2)
            company_name = cols[1].text[2:]
            country = cols[2].text
            status = cols[3].text
            website = cols[6].text
            vat_number = cols[7].text
            sic_code = cols[8].text
            sic_desc = cols[9].text
            industry = cols[10].text
            legal_form = cols[11].text
            reg_no = cols[12].text
            address = cols[13].text
            alexa_rank = cols[14].text
            monthly_visits = cols[15].text
            employees = cols[16].text
            turnover = cols[17].text
            age = cols[18].text
            data.append([
                company_name,
                country,
                status,
                # company_email,
                # phone,
                website,
                vat_number,
                sic_code,
                sic_desc,
                industry,
                legal_form,
                reg_no,
                address,
                alexa_rank,
                monthly_visits,
                employees,
                turnover,
                age,
            ])
        #save_data_to_db(data)
        save_to_file(data)

    def scrape_companies_data_copy(self):
        self.scroll()
        print('scrolled')
        table = self.driver.find_element("xpath", '//table/tbody')
        rows = table.find_elements("tag name", "tr")
        print('TOTAL number of rows', len(rows))

            # Get the new rows
            #for i in range(7):
            #    self.scroll()
        data = self.data_processing()
        return data
    def scrape_companies_data(self):
        table = self.driver.find_element("xpath", '//table/tbody')
        rows = table.find_elements("tag name", "tr")
        last_row = rows[-1]
        last_row_key = last_row.get_attribute("data-row-key")
        print('number of rows', len(rows))
        data = []

        # Add column names to data
        data.append([
            "Company Name",
            "Country",
            "Status",
            "Website",
            "VAT Number",
            "SIC Code",
            "SIC Desc",
            "Industry",
            "Legal Form",
            "Reg No",
            "Address",
            "Alexa Rank",
            "Monthly Visits",
            "Employees",
            "Turnover",
            "Age"
        ])
        start = 1
        last = 10
        while True:
            for row in rows[start:last]:
                cols = row.find_elements("tag name", "td")
                time.sleep(2)
                company_name = cols[1].text[2:]
                country = cols[2].text
                status = cols[3].text
                #company_email = cols[4].text
                #phone = cols[5].text
                website = cols[6].text
                vat_number = cols[7].text
                sic_code = cols[8].text
                sic_desc = cols[9].text
                industry = cols[10].text
                legal_form = cols[11].text
                reg_no = cols[12].text
                address = cols[13].text
                alexa_rank = cols[14].text
                monthly_visits = cols[15].text
                employees = cols[16].text
                turnover = cols[17].text
                age = cols[18].text
                data.append([
                    company_name,
                    country,
                    status,
                    #company_email,
                    #phone,
                    website,
                    vat_number,
                    sic_code,
                    sic_desc,
                    industry,
                    legal_form,
                    reg_no,
                    address,
                    alexa_rank,
                    monthly_visits,
                    employees,
                    turnover,
                    age,
                ])

            print(len(data))

            if last < 50:
                start = last
                last += 10
            else:
                save_data_to_db(data)
                data = []
                # Get the new rows
                #for i in range(7):
                #    self.scroll()
                # Wait for the new rows to load
                time.sleep(2)
                element = self.driver.find_element("xpath",
                                                 f'//table/tbody/tr[@data-row-key="{last_row_key}"]')
                # Scroll the web page to the element using JavaScript
                self.driver.execute_script('arguments[0].scrollIntoView(true);', element)
                table = self.driver.find_element("xpath", '//table/tbody')
                print("-----------else started")
                print("table", table)
                rows = table.find_elements("tag name", "tr")
                print('rows', rows)
                print('rows len', len(rows))
                last_row = rows[-1]
                last_row_key = last_row.get_attribute("data-row-key")
                last = 10
                start = 1

            # If there are no new rows, stop scrolling
            if len(rows) == 0:
                break
            print(data[-2:])
        return data

def clean_data(data):
    cleaned_data = []
    for row in data:
        # Remove commas and "Inc" from company name
        company_name = row[0].replace(",", "").replace(" Inc", "").strip()

        # Replace missing values with "---"
        for i in range(1, len(row)):
            if row[i] == "":
                row[i] = "---"

        # Remove commas from alexa_rank, monthly_visits, and employees columns
        alexa_rank = row[11].replace(",", "").strip()
        monthly_visits = row[12].replace(",", "").strip()
        employees = row[13].replace(",", "").strip()

        # Add the cleaned data to the list
        cleaned_data.append([
            company_name,
            row[1],  # Country
            row[2],  # Status
            row[3],  # Website
            row[4],  # VAT Number
            row[5],  # SIC Code
            row[6],  # SIC Desc
            row[7],  # Industry
            row[8],  # Legal Form
            row[9],  # Reg No
            row[10],  # Address
            alexa_rank,
            monthly_visits,
            employees,
            row[14],  # Turnover
            row[15],  # Age
        ])
    return cleaned_data

def save_to_file(data):
    with open('my_file.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

def save_data_to_db(data):
    conn = psycopg2.connect(
        host='localhost',
        database="company",
        user="postgres",
        password="admin"
    )
    cur = conn.cursor()

    try:
        cur.execute('CREATE TABLE IF NOT EXISTS companies (' +
                    'company_name VARCHAR(255),' +
                    'country VARCHAR(255),' +
                    'status VARCHAR(255),' +
                    'website VARCHAR(255),' +
                    'vat_number VARCHAR(255),' +
                    'sic_code VARCHAR(255),' +
                    'sic_desc VARCHAR(255),' +
                    'industry VARCHAR(255),' +
                    'legal_form VARCHAR(255),' +
                    'reg_no VARCHAR(255),' +
                    'address VARCHAR(255),' +
                    'alexa_rank VARCHAR(255),' +
                    'monthly_visits VARCHAR(255),' +
                    'employees VARCHAR(255),' +
                    'turnover VARCHAR(255),' +
                    'age VARCHAR(255))')
        conn.commit()
    except Exception as e:
        print(str(e))
        conn.rollback()

    # Clean and process the data
    cleaned_data = data
    #cleaned_data = clean_data(data)

    # Insert the data into the database
    for row in cleaned_data[1:]:
        query = "INSERT INTO companies (company_name, country, status, website, vat_number, sic_code, sic_desc, industry, legal_form, reg_no, address, alexa_rank, monthly_visits, employees, turnover, age) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15])
        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    return cleaned_data

def main():
    # Create scraper object and login
    scraper = GlobalDatabaseScraper('alsabagh@innoscripta.com', 'I$3Gh26TTdg1')
    scraper.login()

    # Go to companies page and filter by country
    time.sleep(5)
    scraper.go_to_companies_page()
    scraper.filter_by_location()
    scraper.filter_by_country('United States')

    # Scrape data
    time.sleep(5)
    point = scraper.driver.find_element("xpath", '//span[@class="gdb-status-dot gdb-status-dot-success"]')
    point.click()
    #data = scraper.scrape_companies_data()
    print('before')
    data = scraper.scrape_companies_data_copy()
    print(len(data))
    #cleaned_data = save_data_to_db(data)


    # Print the data to the console
    #for row in cleaned_data[:2]:
        #print(len(cleaned_data))
    # Quit the browser
    scraper.driver.quit()

if __name__ == '__main__':
    main()

