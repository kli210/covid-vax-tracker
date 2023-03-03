# Automate the download of global covid-19 dataset in csv file from the Our World in Data website
import urllib.request
import time

url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
filename = "covid_data.csv"

# Fetch new data and update db daily
while True:
    urllib.request.urlretrieve(url, filename)
    print("csv file successfully downloaded!")

    import pandas as pd
    import mysql.connector as mysql

    # Import CSV
    df = pd.read_csv('C:/Users//XXXXX//covid_data.csv',index_col=False, delimiter = ',')
    final = df.fillna("")

    # Connect to SQL Server
    try:
        with mysql.connect(
                user='root',
                password='', 
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
                cursor.execute("CREATE TABLE covidrecords(iso_code VARCHAR(40), continent VARCHAR(40), location VARCHAR(40), record_date VARCHAR(40), total_cases VARCHAR(40), new_cases VARCHAR(40), new_cases_smoothed VARCHAR(40), total_deaths VARCHAR(40), new_deaths VARCHAR(40), new_deaths_smoothed VARCHAR(40), total_cases_per_million VARCHAR(40), new_cases_per_million VARCHAR(40), new_cases_smoothed_per_million VARCHAR(40), total_deaths_per_million VARCHAR(40), new_deaths_per_million VARCHAR(40), new_deaths_smoothed_per_million VARCHAR(40), reproduction_rate VARCHAR(40), icu_patients VARCHAR(40), icu_patients_per_million VARCHAR(40), hosp_patients VARCHAR(40), hosp_patients_per_million VARCHAR(40), weekly_icu_admissions VARCHAR(40), weekly_icu_admissions_per_million VARCHAR(40), weekly_hosp_admissions VARCHAR(40), weekly_hosp_admissions_per_million VARCHAR(40), total_tests VARCHAR(40), new_tests VARCHAR(40), total_tests_per_thousand VARCHAR(40), new_tests_per_thousand VARCHAR(40), new_tests_smoothed VARCHAR(40), new_tests_smoothed_per_thousand VARCHAR(40), positive_rate VARCHAR(40), tests_per_case VARCHAR(40), tests_units VARCHAR(40), total_vaccinations VARCHAR(40), people_vaccinated VARCHAR(40), people_fully_vaccinated VARCHAR(40), total_boosters VARCHAR(40), new_vaccinations VARCHAR(40), new_vaccinations_smoothed VARCHAR(40), total_vaccinations_per_hundred VARCHAR(40), people_vaccinated_per_hundred VARCHAR(40), people_fully_vaccinated_per_hundred VARCHAR(40), total_boosters_per_hundred VARCHAR(40), new_vaccinations_smoothed_per_million VARCHAR(40), new_people_vaccinated_smoothed VARCHAR(40), new_people_vaccinated_smoothed_per_hundred VARCHAR(40), stringency_index VARCHAR(40), population_density VARCHAR(40), median_age VARCHAR(40), aged_65_older VARCHAR(40), aged_70_older VARCHAR(40), gdp_per_capita VARCHAR(40), extreme_poverty VARCHAR(40), cardiovasc_death_rate VARCHAR(40), diabetes_prevalence VARCHAR(40), female_smokers VARCHAR(40), male_smokers VARCHAR(40), handwashing_facilities VARCHAR(40), hospital_beds_per_thousand VARCHAR(40), life_expectancy VARCHAR(40), human_development_index VARCHAR(40), population VARCHAR(40), excess_mortality_cumulative_absolute VARCHAR(40), excess_mortality_cumulative VARCHAR(40), excess_mortality VARCHAR(40), excess_mortality_cumulative_per_million VARCHAR(40))")
                print("\nTable is created")
                
            # Loop through DataFrame
            
            for i,row in final.iterrows():
                    sql = "INSERT INTO vaxtracker.covidrecords VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    print(" Record inserted")
                    # The connection is not auto-committed by default thus must commit to save changes
                    cnx.commit()
            print("\ncsv file successfully imported!")
            cursor.execute("SELECT * FROM covidrecords")
            cursor.fetchall()
            print("Total number of rows in table: ", cursor.rowcount)
        print("\nMySQL connection is closed.")
        time.sleep(86400)  # Waits for 24 hours before downloading csv file and loading it to the db again
                    
    except OSError as e:
        print("Error while connecting to MySQL", e)





    
            
            


