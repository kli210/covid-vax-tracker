# Automate the download of global covid-19 dataset in csv file from the Our World in Data website
import urllib.request
import time

url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
filename = "covid_data.csv"

while True:
    urllib.request.urlretrieve(url, filename)
    print("csv file successfully downloaded!")

    import pandas as pd
    import mysql.connector as mysql

    # Import CSV
    df = pd.read_csv('C:/Users/kelly/Desktop/data_automation/test.csv',index_col=False, delimiter = ',')
    final = df.fillna("")

    # Connect to SQL Server
    try:
        with mysql.connect(
                user='root',
                password='Molecule0!', 
                host='localhost', 
                database='vaxtracker'
                ) as cnx:
            if cnx.is_connected():
                cursor = cnx.cursor()
                cursor.execute("SELECT database();")
                record = cursor.fetchone()
                print("\nYou're connected to database: ", record)
                cursor.execute("DROP TABLE IF EXISTS covidrecords;")
                print("\nCreating table..")
                cursor.execute("CREATE TABLE covidrecords (name VARCHAR(40), birthday VARCHAR(40), grade VARCHAR(40), school VARCHAR(40))")
                print("\nTable is created")

            # Loop through DataFrame
            
            for i,row in final.iterrows():
                    sql = "INSERT INTO vaxtracker.covidrecords VALUES (%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    print(" Record inserted")
                    # The connection is not auto-committed by default thus must commit to save changes
                    cnx.commit()
            print("\ncsv file successfully imported!")
            cursor.execute("SELECT * FROM covidrecords")
            cursor.fetchall()
            print("Total number of rows in table: ", cursor.rowcount)
        print("\nMySQL connection is closed.")
        time.sleep(86400)  # Waits for 24 hours before downloading csv file and importing it to db again
                
    except OSError as e:
        print("Error while connecting to MySQL", e)
    

# Data stays up-to-date daily



    
            
            


