#automate the download of global covid-19 dataset in csv file from the Our World in Data website
import urllib.request
import time

url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
filename = "covid_data.csv"

while True:
    urllib.request.urlretrieve(url, filename)
    print("File successfully downloaded!")
    time.sleep(86400)  # waits for 24 hours before downloading again
    


