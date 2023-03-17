"""The script imports the necessary modules and libraries: sqlite3 for interacting with
the SQLite database, datetime for parsing and formatting dates, requests for making HTTP
requests to the Amazon pages, and BeautifulSoup for parsing the HTML response from Amazon.

The script creates two tables in a SQLite database called amazon.db. The first table is called products and has two
columns: Product_id (an integer primary key) and name (a text field for storing the product name). The second table
is called reviews and has six columns: SID (an integer primary key), Product_id (a foreign key referencing the
Product_id column in the products table), User (a text field for storing the username of the reviewer), Date (a date
field for storing the date of the review), Message (a text field for storing the review content), and Sentiment (a
text field for storing the sentiment of the review, which is not used in this script).

The script reads in a file called terms.txt, which contains a list of Amazon product URLs. It then iterates over each
URL and scrapes the product name, usernames, dates, and review content from the first 10 pages of reviews for that
product.

The script formats the dates in the reviews to a consistent format (dd/mm/yyyy), and stores the scraped data in four
lists: prod_name (the product name), Users (a list of usernames), Dates (a list of formatted dates), and Messages (a
list of review content).

The script inserts the product name into the products table, and retrieves the Product_id value for that row.

The script creates a list of tuples containing the Product_id, username, date, and review content for each review,
    and inserts them into the reviews table using a SQL INSERT statement.

The script repeats steps 5 and 6 for each URL in the terms.txt file."""

# Necessary imports

import sqlite3
import datetime
import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------
# ETL from Amazon, Otabek Askarov.


# Step 1: Create the database and tables.

# Connect to the database
conn = sqlite3.connect('amazon.db')

# Create the tables
conn.execute('''
CREATE TABLE IF NOT EXISTS products (
  Product_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
''')

# Create the tables
conn.execute('''CREATE TABLE IF NOT EXISTS reviews (SID INTEGER PRIMARY KEY,
                                                             Product_id INTEGER,
                                                             User TEXT,
                                                             Date DATE,
                                                             Message TEXT,
                                                             Sentiment TEXT,
                                                             FOREIGN KEY (Product_id) REFERENCES products(Product_id));''')

# Commit the changes
conn.commit()

# Close the connection
conn.close()

# Step 2: extracting the links from terms.txt file

links = []  # links to the products are stored in this list

with open('terms.txt', 'r') as f:
    lines = f.readlines()
    for line in lines:
        link = line.strip()
        links.append(link)


# Step 3: Scrape the product reviews, usernames, and dates.

def scrape_reviews(urlbase, numpages):
    # Initialize lists to store the scraped data
    usernames = []
    dates = []
    review_content = []

    # Set the headers for the HTTP request
    headers = {
        'User-Agent': 'Mozilla / 5.0(Windows NT10.0; Win64;x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 108.0.0.0 Safari / 537.36'}

    # Iterate over the pages
    for i in range(1, numpages + 1):
        # Construct the URL for the current page
        url = urlbase + str(i)

        # Send the HTTP request and get the page content
        page = requests.get(url, headers=headers)
        bs = BeautifulSoup(page.content, "html.parser")

        # Extract the product name from the page
        product_name = bs.find("h1", {"class": "a-size-large a-text-ellipsis"}).get_text()

        # Extract the usernames from the page
        names = bs.find_all("span", class_="a-profile-name")
        for i in range(0, len(names)):
            usernames.append(names[i].get_text())

        # Extract the dates from the page and format them
        raw_date = bs.find_all("span", class_="review-date")
        for i in range(0, len(raw_date)):
            dates.append(raw_date[i].get_text())

        formatted_date_list = []
        for date in dates:
            date_string = date.split("on")[1]
            date_object = datetime.datetime.strptime(date_string, ' %d %B %Y')
            formatted_date = date_object.strftime('%d/%m/%Y')
            formatted_date_list.append(formatted_date)

        # Extract the review content from the page
        review = bs.find_all("span", {"data-hook": "review-body"})
        for i in range(0, len(review)):
            review_content.append(review[i].get_text())
        review_content[:] = [reviews.lstrip('\n') for reviews in review_content]
        review_content[:] = [reviews.rstrip('\n') for reviews in review_content]
        # Remove the first two elements from each list (they are not actual reviews)

    # Slice the lists to the same length as the smallest list
    smallest_list = len(review_content)
    usernames = usernames[:smallest_list]
    formatted_date_list = formatted_date_list[:smallest_list]
    return (product_name, usernames, formatted_date_list, review_content)


# Set the number of pages you want to scrape
num_pages = 10

for url in links:
    prod_name, Users, Dates, Messages = scrape_reviews(url, num_pages)

    final_list = []
    for i in range(len(Messages)):
        final_list.append((Users[i], Dates[i], Messages[i]))

    # Step 4: Insert the product names, product reviews, usernames, and dates into the tables created in Step 1.

    # Connect to the database
    conn = sqlite3.connect('amazon.db')

    # Insert a new row into the products table
    conn.execute("INSERT INTO products (name) VALUES (?)", (prod_name,))

    # Retrieve the product_id for the newly inserted row
    cursor = conn.execute("SELECT LAST_INSERT_ROWID()")

    # Get the product_id value from the cursor
    product_id = cursor.fetchone()[0]

    # Close the cursor
    cursor.close()

    values = [(product_id, user, date, message) for user, date, message in final_list]

    # Insert the values into the table
    conn.executemany('''INSERT INTO reviews (Product_id, User, Date, Message) VALUES (?, ?, ?, ?)''', values)

    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()
