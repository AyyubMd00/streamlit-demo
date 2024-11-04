import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px

# Snowflake connection configuration
def get_snowflake_connection():
    return snowflake.connector.connect(
        user='ayyub',
        password='ayyUB2000',
        account='ab09706.ap-southeast-1',
        warehouse='compute_wh',
        database='nyc_taxi',
        schema='public'
    )

# Query to fetch data from Snowflake
def fetch_data_from_snowflake(time_filter, region_filter):
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Get filter value from the dictionary
    time_condition = {
        "30 mins": "MINUTE, -30",
        "1 hour": "HOUR, -1",
        "1 day": "DAY, -1",
        "7 days": "DAY, -7"
    }.get(time_filter, "MINUTE, -30")

    query = f"""
    SELECT
        trip_id,
        driver_id,
        passenger_count,
        trip_distance,
        passenger_wait_time,
        pickup_time,
        dropoff_time,
        region,
        fare,
        TIMESTAMPDIFF(minute, pickup_time, dropoff_time) AS trip_duration
    FROM taxi_trips
    WHERE pickup_time >= DATEADD({time_condition}, CURRENT_TIMESTAMP())
    """
    if region_filter != 'All':
        query += f" AND region = '{region_filter}'"
    print(query)
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Close the connection
    cursor.close()
    conn.close()

    # Convert the result into a Pandas DataFrame
    return pd.DataFrame(rows, columns=['trip_id', 'driver_id', 'passenger_count', 'trip_distance', 'passenger_wait_time', 'pickup_time', 'dropoff_time', 'region', 'fare', 'trip_duration'])

# Streamlit App
def main():
    st.set_page_config(layout="wide")
    st.title("Taxi Trips Dashboard")

    # Filter options
    time_filter = st.selectbox("Select Time Range", ["30 mins", "1 hour", "1 day", "7 days"])
    region_filter = st.selectbox("Select Region", ['All', 'North', 'South', 'East', 'West', 'Central'])
    print(time_filter)
    # Fetch data
    data = fetch_data_from_snowflake(time_filter, region_filter)
    print(data.head())
    if not data.empty:
        # KPIs
        total_trips = data['trip_id'].nunique()
        total_revenue = data['fare'].sum()
        total_passengers = data['passenger_count'].sum()
        avg_waiting_time = data['passenger_wait_time'].mean()
        avg_trip_duration = data['trip_duration'].mean()
        total_trip_distance = data['trip_distance'].sum()

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])  # Allocate 1/3 space for KPIs and 2/3 for charts

        # Display KPIs
        with col1:
            # st.markdown(
            #     f"""
            #     <div style="background-color: #f4f4f4; padding: 10px; border-radius: 5px;">
            #         <h3 style="color: #4CAF50;">Total Trips</h3>
            #         <h1 style="color: #333;">{total_trips}</h1>
            #     </div>
            #     """, unsafe_allow_html=True
            # )
            st.metric("Total Trips", total_trips)
            # st.metric("Total Drivers", total_drivers)
            st.metric("Total Passengers", total_passengers)
            st.metric("Total Revenue", total_revenue)
            st.metric("Total Trip Distance (km)", round(total_trip_distance, 2))
            st.metric("Avg. Trip Duration (mins)", round(avg_trip_duration, 2))
            st.metric("Avg. Waiting Time (mins)", round(avg_waiting_time, 2))

        with col2:
            # Pie Chart: Total number of trips by region
            trip_count_by_region = data.groupby('region')['trip_id'].nunique().reset_index()
            fig1 = px.pie(trip_count_by_region, values='trip_id', names='region', title="Total Trips by Region")
            st.plotly_chart(fig1)

        with col3:
            # Pie Chart: Total revenue by region
            revenue_by_region = data.groupby('region')['fare'].sum().reset_index()
            fig2 = px.pie(revenue_by_region, values='fare', names='region', title="Total Revenue by Region")
            st.plotly_chart(fig2)

        with col4:
            # Bar Chart: Revenue per trip by region
            revenue_per_trip_by_region = revenue_by_region.copy()
            revenue_per_trip_by_region['revenue_per_trip'] = revenue_by_region['fare'] / trip_count_by_region['trip_id']
            fig3 = px.bar(revenue_per_trip_by_region, x='region', y='revenue_per_trip', title="Revenue per Trip by Region")
            st.plotly_chart(fig3)

        # display data optionally
        if st.checkbox("Show raw data"):
            st.write(data)

    else:
        st.write("No data available for the selected time range.")

if __name__ == "__main__":
    main()