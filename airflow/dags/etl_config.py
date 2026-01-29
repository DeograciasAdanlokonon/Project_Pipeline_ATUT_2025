from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import csv
import os
import logging
import zipfile
from io import BytesIO
import kagglehub
from minio import Minio
from minio.error import S3Error

input_folder = 'input_files'  # for downloaded files
output_folder = 'output_files'  # for extracted files


def extract_data_from_books_to_scraped():
  """
  Scrapes the web page books.toscrape.com and 
  extracts all the books details
  """

  logging.info('[INFO] Start Scraping Process!')

  url = "https://books.toscrape.com/"
  extracted_file = "data_scraped_books.csv"

  try:
    response = requests.get(url=url, timeout=10)
    response.raise_for_status()
    home_page = response.text
    soup = BeautifulSoup(home_page, "html.parser")

    next_page = True  # Initiate a flag for existant next page
    page = 1

    # TODO: #1 - Create a CSV file and write header
    with open(extracted_file, "w", newline="", encoding="utf-8") as outfile:
      writer = csv.writer(outfile)  # CSV Writer

      # Write header
      header = ['Upc', 'Title', 'Description', 'Price', 'Stock Available', 'Image URL']
      writer.writerow(header)

    # TODO: #2 - Run scraping while next page exist
    while next_page:
      catalog = soup.find("ol", class_="row")
      if not catalog:
        break
      
      # Get all articles in soup
      articles = catalog.find_all("article", class_="product_pod")

      # Loop through each article on a page
      for article in articles:
        # Get current article link
        article_endpoint = article.find('div', class_="image_container").find('a').get('href')
        if 'catalogue/' in article_endpoint:
          article_link = urljoin(url, article_endpoint)
        else:
          article_link = urljoin(f'{url}catalogue/', article_endpoint)
        
        # Get current article page
        article_page = requests.get(url=article_link, timeout=10).text
        article_soup = BeautifulSoup(article_page, "html.parser")

        ### Article details ###
        # Title
        title = article_soup.find("h1").getText(strip=True)

        # Descrption
        paragraphs = article_soup.find("article", class_="product_page").find_all("p")
        description = (
          paragraphs[3].get_text(strip=True) if len(paragraphs) > 3 else ""
        )

        # Price
        price = article_soup.find("div", class_="product_main").find('p').getText(strip=True).replace('Â', '')

        # Stock
        stock = article_soup.find('p', class_="instock").getText().split('(')[1].replace('available)', '').strip()

        # Image URL
        image_url = urljoin(url, article_soup.find('div', class_="item active").find("img").get("src"))
        
        # UPC
        upc = article_soup.find('table', class_='table table-striped').find('td').getText(strip=True)

        new_book = [upc, title, description, price, stock, image_url]

        # TODO: #3 - Insert each new book in csv file
        with open(extracted_file, 'a', newline='', encoding='utf-8') as outfile:
          writer = csv.writer(outfile)
          writer.writerow(new_book)

      print(f"[INFO] Extraction Page {page} Done")

      # Check for a next page on the current page
      next_tag = soup.find("li", class_="next")
      if next_tag:
          next_endpoint = next_tag.a["href"]
          if 'catalogue/' in next_endpoint:
            next_link = urljoin(url, next_endpoint)
          else:
            next_link = urljoin(f'{url}catalogue/', next_endpoint)

          soup = BeautifulSoup(
              requests.get(next_link, timeout=10).text,
              "html.parser"
          )
          page += 1
      else:
          next_page = False
        
    logging.info('[INFO] Scraping & Extraction Books-To-Scraped Successfully Done!')

  except Exception as e:
    logging.exception(f"[ERROR] Something went wrong: {e}")
  
def download_file_from_google_drive():
  """
  Downloads and unzippes files from a Google Drive URL
  """

  file_id = "1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD"
  url = f"https://drive.google.com/uc?id={file_id}&export=download"

  # Ensure the destination directory exists
  if not os.path.exists(input_folder):
    os.makedirs(input_folder)

  try:
    # Send a GET request to URL
    response = requests.get(url, stream=True, timeout=10)
    response.raise_for_status()

    # Read the content into a BytesIO object for unzipping in memory
    zip_file_bytes = BytesIO(response.content)
    
    # Unzip the file
    with zipfile.ZipFile(zip_file_bytes, 'r') as zip_ref:
      file_names = zip_ref.namelist()
      zip_ref.extractall(input_folder)
    
    return f"{input_folder}/{file_names[0]}"  # Return extracted file path

  except Exception as e:
    logging.exception(f"[ERROR] Something went wrong: {e}")

def extract_data_from_google_drive_books_csv():
  """
  Extracts data from the downloaded Google Drive CSV file
  """
  logging.info('[INFO] Starting Google Drive Extraction Process!')

  input_file = download_file_from_google_drive()
  extracted_file = "data_csv_books.csv"

  if not input_file:  # Exit none if input_file does not exist
    return

  try:
    with open(input_file, "r", encoding="utf-8") as infile, \
    open(extracted_file, "w", newline="", encoding="utf-8") as outfile:
      
      reader = csv.DictReader(infile)  # CSV Reader
      writer = csv.writer(outfile)  # CSV Writer

      # Write header
      header = ["Title", "Author", "Publisher", "Year Published", "Image URL"]
      writer.writerow(
        header
      )

      # Write new lines of books
      for row in reader:
        new_book = [
          row.get("Book-Title"),
          row.get("Book-Author"),
          row.get("Publisher"),
          row.get("Year-Of-Publication"),
          row.get("Image-URL-S")
        ]
        writer.writerow(new_book)

    logging.info(f"[INFO] Extraction Google Drive Books.csv Successfully Done!")

  except Exception as e:
    logging.exception(f"[ERROR] Something went wrong during extraction: {e}")

def download_books_from_kagglehub():
  """
  Downloads books data from KaggleHub
  """
  try:
    # Download latest version
    path = kagglehub.dataset_download("elvinrustam/books-dataset")
    return path
  except Exception as e:
    logging.exception(f"[ERROR] Something went wrong: {e}")
    return None

def extract_data_from_kaggle_books():
  """
  Extracts data from Kaggle books dataset
  """
  logging.info('[INFO] Starting Kaggle Extraction Process!')

  path = download_books_from_kagglehub()
  if not path:
    return
  
  input_file = f"{path}/BooksDatasetClean.csv"
  extracted_file = "data_kaggle_books.csv"

  try:
    with open(input_file, "r", encoding="utf-8") as infile, \
      open(extracted_file, "w", newline="", encoding="utf-8") as outfile:

      reader = csv.DictReader(infile)  # CSV Reader
      writer = csv.writer(outfile)

      # Write header
      header = ["Title", "Author", "Description", "Publisher", "Price", "Year Published"]
      writer.writerow(
        header
      )

      # Write new lines of books
      for row in reader:
        new_book = [
          row.get("Title"),
          row.get("Authors"),
          row.get("Description"),
          row.get("Publisher"),
          row.get("Price Starting With ($)"),
          row.get("Publish Date (Year)")
        ]
        writer.writerow(new_book)

    logging.info(f"[INFO] Extraction Kaggle Books Data Successfully Done!")

  except Exception as e:
    logging.exception(f"[ERROR] Something went wrong: {e}")

def consolidates_all_extracted_file():
  """
  Transforms and consolidates all extracted csv data books into one csv file
  """
  logging.info('[INFO] Starting Consolidation Process!')

  sources = {
    "csv": {
      "file": "data_csv_books.csv",
      "source": "CSV",
      "mapping": lambda r: {
        "Title": r["Title"],
        "Author": r["Author"],
        "Publisher": r["Publisher"],
        "Year Published": r["Year Published"],
        "Image URL": r.get("Image URL"),
      }
    },
    "kaggle": {
      "file": "data_kaggle_books.csv",
      "source": "Kaggle",
      "mapping": lambda r: {
        "Title": r["Title"],
        "Author": r["Author"],
        "Description": r["Description"],
        "Publisher": r["Publisher"],
        "Price": r["Price"],
        "Year Published": r["Year Published"],
      }
    },
    "scraping": {
      "file": "data_scraped_books.csv",
      "source": "Scraping",
      "mapping": lambda r: {
        "Upc": r["Upc"],
        "Title": r["Title"],
        "Description": r["Description"],
        "Price": r["Price"],
        "Stock Available": r["Stock Available"],
        "Image URL": r["Image URL"],
      }
    }
  }
  
  output_file = "etl_books.csv"
  header = ["Id", "Title", "Description", "Author", "Publisher", "Year Published", "Price", "Image URL", "Source"]


  try:
    # TODO - 1: Create Output file and a header
    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
      writer = csv.DictWriter(outfile, fieldnames=header)
      writer.writeheader()

      book_id = 1
    
      # TODO - 2: Go through each file and each row

      for cfg in sources.values():
        with open(cfg["file"], newline="", encoding="utf-8") as infile:
          reader = csv.DictReader(infile)

          for row in reader:
            mapped = cfg["mapping"](row)

            consolidated = {
              "Id": book_id,
              "Title": mapped.get("Title"),
              "Description": mapped.get("Description"),
              "Author": mapped.get("Author"),
              "Publisher": mapped.get("Publisher"),
              "Year Published": mapped.get("Year Published"),
              "Price": mapped.get("Price"),
              "Image URL": mapped.get("Image URL"),
              "Source": cfg["source"],
            }

            writer.writerow(consolidated)
            book_id += 1

    logging.info(f"[INFO] Consolidation Successfully Done!")

  except Exception as e:
    logging.exception(f"[ERROR] Something went wrong during consolidation: {e}")


def load_books_to_minio():
  """
  Loads consolidated books dataset into MinIO with partitioning by Source
  """

  MINIO_ENDPOINT = "minio:9000"
  ACCESS_KEY = "minioadmin"
  SECRET_KEY = "minioadmin"
  BUCKET_NAME = "books-data"

  client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
  )

  if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

  input_file = "etl_books.csv"
  partitions = {}

  # Read consolidated CSV
  with open(input_file, newline="", encoding="utf-8") as infile:
    reader = csv.DictReader(infile)

    for row in reader:
        source = row["Source"]
        partitions.setdefault(source, []).append(row)

  # Write and upload each partition
  for source, rows in partitions.items():
    partition_file = f"books_{source.lower()}.csv"

    with open(partition_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    object_name = f"source={source}/{partition_file}"

    client.fput_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        file_path=partition_file
    )

    logging.info(f"[INFO] Uploaded {object_name} to MinIO")

    os.remove(partition_file)

  logging.info("[INFO] Load to MinIO completed successfully!")

