# Automate the download of global covid-19 dataset in csv file from the Our World in Data website
import urllib.request
import time

url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
filename = "covid_data.csv"

while True:
    urllib.request.urlretrieve(url, filename)
    print("csv file successfully downloaded!")
    time.sleep(86400)  # Waits for 24 hours before downloading again

# Downloads up-to-date csv file daily   


