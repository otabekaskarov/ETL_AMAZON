# ETL_AMAZON
Extract Transform and Load from Amazon website


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
