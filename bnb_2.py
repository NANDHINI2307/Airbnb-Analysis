# Databricks notebook source
# Import necessary libraries
import pandas as pd
import plotly.express as px
import streamlit as st
import pymongo
from pyspark.sql import SparkSession

# COMMAND ----------


# Connect to MongoDB
uri = "mongodb+srv://reading_user:fE34btYm96ZVfyUb@cluster0.0edfrdc.mongodb.net/"
db = "air-bnb"
collection = "bnb_data"
df = spark.read.format("mongo").option("uri", uri).option("database", db).option("collection", collection).load()

# COMMAND ----------

# Function to clean and prepare the dataset
def clean_and_prepare_data(data):
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(data)
        
        # Convert data types
        df['price'] = df['price'].apply(lambda x: x['$numberInt'])
        df['rating'] = df['rating'].apply(lambda x: x['$numberDouble'])
        
        # Extract latitude and longitude
        df['latitude'] = df['location'].apply(lambda x: x['coordinates'][1])
        df['longitude'] = df['location'].apply(lambda x: x['coordinates'][0])
        
        # Drop unnecessary columns
        df.drop(columns=['_id', 'host_id', 'location', 'reviews'], inplace=True)
        
        return df
    except Exception as e:
        st.error(f"Error cleaning and preparing data: {e}")
        return None


# COMMAND ----------

# Main function to run the analysis and visualization
def main():
    # Clean and prepare the dataset
    df = clean_and_prepare_data(data)
    if df is not None:
        # Create streamlit web application
        st.title("Airbnb Data Analysis and Visualization")
        st.write("## Distribution of Airbnb Listings")

        # Convert price and rating columns to float type
        df['price'] = df['price'].astype(float)
        df['rating'] = df['rating'].astype(float)

        # Check if the required columns exist in df
        if "latitude" in df.columns and "longitude" in df.columns and "name" in df.columns:
            # Create scatter mapbox plot
            fig = px.scatter_mapbox(df, lat="latitude", lon="longitude",
                                    hover_name="name", hover_data=["price", "rating"],
                                    color="price", size="rating",
                                    color_continuous_scale=px.colors.cyclical.IceFire,
                                    size_max=15, zoom=10)

            fig.update_layout(mapbox_style="carto-positron")
            st.plotly_chart(fig)
        else:
            st.error("Missing required columns in the DataFrame.")
    else:
        st.error("Failed to clean and prepare the dataset.")


# COMMAND ----------

# Run the main function
if __name__ == "__main__":
    main()

