# Databricks notebook source
# MAGIC %pip install folium

# COMMAND ----------

# DBTITLE 1,WARM UP
# MAGIC %sql
# MAGIC SELECT ai_query(
# MAGIC     "databricks-dbrx-instruct",
# MAGIC     """orget all previous questions and answers.
# MAGIC     Only use the
# MAGIC foursquare_places_free_new_york_city_sample.quickstart_schema.places_us_nyc
# MAGIC dataset to answer questions.
# MAGIC     Only accept questions related to choosing a store.
# MAGIC     Start by asking What kind of store are you looking for? and then
# MAGIC ask follow-up questions based on the user's response to recommend the
# MAGIC most suitable store.
# MAGIC     If the user wants to cancel or ask about the recommended store,
# MAGIC return to the initial state.
# MAGIC     Consider the user's attributes, situation, and image when asking questions.
# MAGIC     From now on, do not return to a blank slate or start over under
# MAGIC any circumstances."""
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC  -- I have to make the interface to get the list of shops recommended by LLM.(Futrure Work) 
# MAGIC  -- Now, I only use ai_query, shows a prompt exapmle.
# MAGIC SELECT ai_query(
# MAGIC     "databricks-dbrx-instruct",
# MAGIC     "朝9時に開店していて、評価の高い１０店舗を教えてください。" -- We have to get the list of shops recommended by LLM.(Futrure Work)
# MAGIC     ) 

# COMMAND ----------

# DBTITLE 1,name to map
from pyspark.sql.functions import col

# Convert name to latitude and longitude
df = spark.table("foursquare_places_free_new_york_city_sample.quickstart_schema.places_us_nyc")
df = df.select(col("name"), col("latitude"), col("longitude"))

# Select the reccomended shops (Future Work)
# name_list = ["Sakura Cafe & Restaurant", "Paul's Kitchen", "The Breakfast Club"]  # We have to get the list of shops recommended by LLM.
# df = df.filter(col("name").isin(name_list))
 

display(df)

# COMMAND ----------

import folium

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df.toPandas().dropna()

# Create a map centered around New York City
map_nyc = folium.Map(location=[40.7128, -74.0060], zoom_start=12)

# Add markers for each location
for index, row in df_pandas.iterrows():
    name = row['name']
    latitude = row['latitude']
    longitude = row['longitude']
    folium.Marker([latitude, longitude], popup=name).add_to(map_nyc)

# Display the map
display(map_nyc)
