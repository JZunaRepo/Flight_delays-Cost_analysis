import psycopg2
import pandas as pd 
from config import params

conn = psycopg2.connect(**params)

# cursor connection
cursor = conn.cursor()
cursor.execute("SELECT * FROM real_flight WHERE cancelled = 'False' and diverted = 'False'")
rows = cursor.fetchall()
cursor.close()
conn.close()

df = pd.DataFrame(rows, columns=[desc.name for desc in cursor.description])
cleaned_df = df.dropna(subset=['arr_del15', 'dep_del15'])

#Classifying each delayed arilne by labeling 1 if either `ARR_DEL15` or `DEP_DEL15` or 0 if none were delayed
cleaned_df["Delayed"] = int(0)
cleaned_df.loc[cleaned_df["arr_del15"] == True, "Delayed"] = int(1)
cleaned_df.loc[cleaned_df["dep_del15"] == True, "Delayed"] = int(1)

#Creating groups to find the ration per airline of delayed flights
delayed_ratio_airline = cleaned_df.groupby("op_unique_carrier")["Delayed"].value_counts().reset_index(name="ratio")
delayed_ratio_airline["Ratio_Percentage"] = round((delayed_ratio_airline["ratio"] / delayed_ratio_airline.groupby('op_unique_carrier')["ratio"].transform('sum')) * 100,2)
sort_airline_ratio = delayed_ratio_airline.sort_values(by="Ratio_Percentage",ascending=False)
sort_airline_ratio.to_csv('delayed_airlines.csv')

#Creating groups to find the ration per airport of delayed flights
delayed_ratio_airport = cleaned_df.groupby(["origin_airport_id","op_unique_carrier"])["Delayed"].value_counts().reset_index(name="ratio")
delayed_ratio_airport["Ratio_Percentage"] = round((delayed_ratio_airline["ratio"] / delayed_ratio_airline.groupby('op_unique_carrier')["ratio"].transform('sum')) * 100,2)
sort_airport_ratio = delayed_ratio_airport.sort_values(by="Ratio_Percentage",ascending=False)
sort_airport_ratio.to_csv('delayed_airports.csv')