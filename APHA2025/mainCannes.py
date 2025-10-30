import plotly.graph_objects as go
import numpy as np
from datetime import datetime
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import requests
from datetime import datetime, timedelta
import re
import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

#========================================================================================================================================================================
# helper function to pass the date
#========================================================================================================================================================================


def get_date_or_range(preApha, postApha, Apha):
    if preApha:
        return "2025-10-07 to 2025-10-11", "2024-10-22 to 2024-10-26"
    elif postApha:
        return "2025-10-16 to 2025-10-20", "2024-10-31 to 2024-11-04"
    elif Apha is not None:
        Apha_dates = {
            1: ("2025-10-12", "2024-10-27"),
            2: ("2025-10-13", "2024-10-28"),
            3: ("2025-10-14", "2024-10-29"),
            4: ("2025-10-15", "2024-10-30")

        }
        return Apha_dates.get(Apha, "Invalid APHA date")
    else:
        return "Invalid input arguments"

# def get_date_or_range(preApha, postApha, Apha):
#     if preApha:
#         return "2025-10-28 to 2025-11-01", "2024-10-22 to 2024-10-26"
#     elif postApha:
#         return "2025-11-07 to 2025-11-11", "2024-10-31 to 2024-11-04"
#     elif Apha is not None:
#         Apha_dates = {
#             1: ("2025-11-02", "2024-10-27"),
#             2: ("2025-11-03", "2024-10-28"),
#             3: ("2025-11-04", "2024-10-29"),
#             4: ("2025-11-05", "2024-10-30")

#         }
#         return Apha_dates.get(Apha, "Invalid APHA date")
#     else:
#         return "Invalid input arguments"


def get_all_platform_details():
    
    channel_config = {
        "Facebook": {
            "db": "social_media_fb",
            "table": "page_follows_initial",
            "column": "valuedata",
            "condition": "channel_id = 2",
            "date_column": "end_time"
        },
        "Instagram": {
            "db": "social_media_insta",
            "table": "followers_count_initial",
            "column": "like_count",
            "condition": "channel_id = 7",
            "date_column": "today_date"
        },
        "Youtube": {
            "db": "youtube",
            "table": "youtube_subscribers_metrics",
            "column": "Subscribers",
            "condition": "ChannelId = 16",
            "date_column": "CreatedOn"
        }
    }
    return channel_config

#========================================================================================================================================================================
# function to fetch and plot followers count
#========================================================================================================================================================================

def apha_followers_count_analysis(preApha=False, postApha=False, Apha=None):
    platform_details = get_all_platform_details()
    results = []

    # Get the date range based on input arguments
    date_or_range = get_date_or_range(preApha, postApha, Apha)

    # If the return value is a tuple, unpack it
    if isinstance(date_or_range, tuple):
        date_or_range = date_or_range[0]
    print(date_or_range)

    # Determine if the input is a single date or a range
    if "to" in date_or_range:
        start_date, end_date = date_or_range.split(" to ")
        start_date = start_date.strip()
        end_date = end_date.strip()
        graph_type = "line"  # Line graph for date ranges
        commentary_date_info = f"from {start_date} to {end_date}"
    else:
        start_date = date_or_range
        end_date = date_or_range
        graph_type = "bar"  # Bar graph for a single date
        commentary_date_info = f"on {date_or_range}"

    print(f"Date range: {start_date} to {end_date}")

    # Loop through each platform and fetch data
    for platform, details in platform_details.items():
        print(f"Fetching data for platform: {platform}")
        try:
            connection = mysql.connector.connect(
                host="45.76.160.28",
                user="Summer",
                password="Dragon",
                database=details["db"]
            )
            
            # For YouTube: use subquery to get the latest entry per date
            if platform.lower() == "youtube":
                query = f"""
                    SELECT t1.{details["column"]}, DATE(t1.{details["date_column"]}) as date_col, CreatedOn
                    FROM {details["table"]} t1
                    INNER JOIN (
                        SELECT DATE({details["date_column"]}) as date_col, MAX({details["date_column"]}) as max_date
                        FROM {details["table"]}
                        WHERE {details["condition"]}
                        AND DATE({details["date_column"]}) BETWEEN %s AND %s
                        GROUP BY DATE({details["date_column"]})
                    ) t2 ON DATE(t1.{details["date_column"]}) = t2.date_col AND t1.{details["date_column"]} = t2.max_date
                    WHERE {details["condition"]}
                    ORDER BY date_col ASC;
                """
            else:
                # For other platforms: use time condition
                query = f"""
                    SELECT 
                        {details["column"]}, 
                        DATE_SUB(DATE({details["date_column"]}), INTERVAL 1 DAY) as date_col, created_on
                    FROM {details["table"]}
                    WHERE {details["condition"]}
                    AND DATE({details["date_column"]}) BETWEEN DATE_ADD(%s, INTERVAL 1 DAY) AND DATE_ADD(%s, INTERVAL 1 DAY)
                    AND TIME(created_on) BETWEEN '11:00:00' AND '11:30:00'
                    GROUP BY DATE({details["date_column"]}), DATE_SUB(DATE({details["date_column"]}), INTERVAL 1 DAY)
                    ORDER BY date_col ASC;
                """

            cursor = connection.cursor()
            cursor.execute(query, (start_date, end_date))
            results_data = cursor.fetchall()

            platform_data = []
            for row in results_data:
                platform_data.append({'Date': row[1], 'Value': row[0]})

            # If data is available, create a DataFrame and plot
            if platform_data:
                df = pd.DataFrame(platform_data)
                print(df)

                if platform.lower() == "youtube":
                    graph_title = f"{platform} Subscribers Count"
                else:
                    graph_title = f"{platform} Followers Count"

                # Generate a graph for the platform
                output_filename = f"{platform}_followers_count.jpeg"
                graph_json = format_followers_graph(df, graph_title, "Date", "Followers/ Subscribers Count", 
                                                   output_filename, graph_type)
                
                # Extract created_on from the first row if available
                created_on_time = None
                if results_data:
                    created_on_dt = results_data[0][2]  # Assuming 'created_on' is at index 2
                    if isinstance(created_on_dt, str):
                        created_on_dt = pd.to_datetime(created_on_dt)
                    created_on_time = created_on_dt.strftime('%I:%M:%S %p on %B %d, %Y')  # Format to readable HKT time


                # Create commentary
                commentary = [f"The above graph displays the followers count for the dates {commentary_date_info} of APHA 2025 taken at {created_on_time} HKT."]
               
                # Append the JSON graph and DataFrame data to results
                results.append({
                    "platform": platform,
                    "graph": graph_json,
                    "commentary": commentary
                })

            cursor.close()
            connection.close()

        except mysql.connector.Error as err:
            print(f"Error for platform {platform}: {err}")

    return results



def format_followers_graph(df, title, x_title, y_title, output_filename, graph_type):
    # Ensure data has unique dates by grouping, just in case of duplicates
    df = df.groupby('Date', as_index=False).agg({'Value': 'sum'})
    min_value = df['Value'].min()
    max_value = df['Value'].max()
    y_axis_min = min_value - (min_value* 0.1)
    y_axis_max = max_value * 1.1  # Extend by 10%

    # Create the plot
    fig = go.Figure()

    if graph_type == "line":
        # Create a line plot
        fig.add_trace(go.Scatter(
            x=df['Date'],
            y=df['Value'],
            mode='lines+markers+text',
            text=[f"{int(val):,}" for val in df['Value']],
            textposition='top center',
            textfont=dict(color='white', size=8),
            line=dict(color='#850000', width=2),
            marker=dict(color='#850000', size=6),
            name='Value'
        ))
    elif graph_type == "bar":
        # Create a bar plot
        fig.add_trace(go.Bar(
            x=df['Date'],
            y=df['Value'],
            text=[f"{int(val):,}" for val in df['Value']],
            textposition='outside',
            marker=dict(color='#850000'),
            name='Value'
        ))

    fig.update_layout(
        plot_bgcolor='black',
        paper_bgcolor='black',
        font=dict(color='#c2a94f'),
        title={
            'text': title,
            'font': {'size': 14, 'color': '#c2a94f'},
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'automargin': True,
            'pad': {'t': 10}
        },
        xaxis=dict(
            title=x_title,
            title_font=dict(family='Roboto', color='#c2a94f', size=10),
            tickfont=dict(family='Roboto', color='#c2a94f', size=10),
            showgrid=True,
            zeroline=False,
            gridcolor='rgba(255,255,255,0.2)',
            showline=True,
            tickmode='array',
            tickvals=df['Date'],  # Explicitly use the dates for ticks
            ticktext=[date.strftime('%b %d, %Y') for date in df['Date']],  # Format date as Month Day
        ),
        yaxis = dict(
            title=y_title,
            title_font=dict(family='Roboto', color='#c2a94f', size=10),
            tickfont=dict(family='Roboto', color='#c2a94f', size=10),
            showgrid=True,
            zeroline=False,
            gridcolor='rgba(255,255,255,0.2)',
            showline=True,
            tickformat=',.0f',  # Format the y-axis as a number (integer)
            range=[y_axis_min, y_axis_max]  # Set the range from 0 to the extended max value
        ),
        margin=dict(l=10, r=10, t=30, b=10)
    )

    return fig.to_json()


#========================================================================================================================================================================
# Function to fetch and plot the actual and projected followers count
#========================================================================================================================================================================

def apha_actual_projected_followers_percentage(preApha=False, postApha=False, Apha=None):
    platform_details = get_all_platform_details()
    results = []

    # Get the date or date range
    date_or_range = get_date_or_range(preApha, postApha, Apha)

    if not date_or_range:
        print("No valid date or range provided.")
        return []  # Early return for invalid dates

    if isinstance(date_or_range, tuple):
        selected_date_or_range = date_or_range[0]
    else:
        selected_date_or_range = date_or_range

    print(f"Fetching data for: {selected_date_or_range}")

    # Handle single or range of dates
    if "to" in selected_date_or_range:
        start_date, end_date = selected_date_or_range.split(" to ")
        date_list = pd.date_range(start=start_date.strip(), end=end_date.strip()).strftime('%Y-%m-%d').tolist()
    else:
        date_list = [selected_date_or_range]

    print("Date list considered is:", date_list)

    for platform, details in platform_details.items():
        platform_data = []
        print(f"Fetching data for platform: {platform}")

        try:
            connection = mysql.connector.connect(
                host="45.76.160.28",
                user="Summer",
                password="Dragon",
                database=details["db"]
            )
            cursor = connection.cursor()

            for date in date_list:
                # --- Current Day Query ---
                if platform.lower() == "youtube":
                    query_current = f"""
                        SELECT {details["column"]}, DATE({details["date_column"]})
                        FROM {details["table"]}
                        WHERE {details["condition"]}
                        AND DATE({details["date_column"]}) = %s
                        ORDER BY CreatedOn DESC LIMIT 1;
                    """
                else:
                    query_current = f"""
                        SELECT {details["column"]}, DATE({details["date_column"]})
                        FROM {details["table"]}
                        WHERE {details["condition"]}
                        AND DATE({details["date_column"]}) = DATE_ADD(%s, INTERVAL 1 DAY)
                        AND TIME(created_on) BETWEEN '11:00:00' AND '11:30:00';
                    """

                cursor.execute(query_current, (date,))
                result_current = cursor.fetchone()

                if result_current:
                    current_follower_count = result_current[0]
                    current_date = date

                    # --- Previous Day Query ---
                    previous_date = pd.to_datetime(current_date) - pd.Timedelta(days=1)
                    formatted_date = previous_date.strftime('%Y-%m-%d')

                    if platform.lower() == "youtube":
                        query_previous = f"""
                            SELECT {details["column"]}
                            FROM {details["table"]}
                            WHERE {details["condition"]}
                            AND DATE({details["date_column"]}) = %s
                            ORDER BY CreatedOn DESC LIMIT 1;
                        """
                    else:
                        query_previous = f"""
                            SELECT {details["column"]}
                            FROM {details["table"]}
                            WHERE {details["condition"]}
                            AND DATE({details["date_column"]}) = DATE_ADD(%s, INTERVAL 1 DAY)
                            AND TIME(created_on) BETWEEN '11:00:00' AND '11:30:00';
                        """

                    cursor.execute(query_previous, (formatted_date,))
                    result_previous = cursor.fetchone()

                    if result_previous:
                        previous_follower_count = result_previous[0]
                        difference = current_follower_count - previous_follower_count

                        percentage_change = 0
                        if previous_follower_count != 0:
                            percentage_change = ((difference) / previous_follower_count) * 100
                            percentage_change = round(percentage_change, 2)

                        platform_data.append({
                            'Date': current_date,
                            'Current Count': current_follower_count,
                            'Previous Count': previous_follower_count,
                            'Value': percentage_change
                        })

            cursor.close()
            connection.close()

            # Create graph if data is available
            if platform_data:
                df = pd.DataFrame(platform_data)
                df['Date'] = pd.to_datetime(df['Date'])

                graph_title = f"{platform} {'Net Subscribers' if platform.lower() == 'youtube' else 'Net Followers'} Percentage Change"
                graph_json = format_percentage_change_graph(df, graph_title, "Date", "Percentage Change")

                # Generate commentary
                commentary = []
                commentary.append(f"The graph plots the Day On Day Growth percentage of the followers count for {formatted_date} : {previous_follower_count:,} to {current_date} : {current_follower_count:,}.")
                commentary.append(f"There is a change in the followers count by {difference:,}")

                
                results.append({
                    'platform': platform,
                    'graph_json': graph_json,
                    'commentary': commentary
                })

        except mysql.connector.Error as err:
            print(f"Error for platform {platform}: {err}")

    return results



def format_percentage_change_graph(df, title, x_label, y_label):
    # Ensure data has unique dates by grouping, just in case of duplicates
    df = df.groupby('Date', as_index=False).agg({'Value': 'sum'})

    # Convert the 'Value' column to represent percentages (i.e., divide by 100 to normalize)
    df['Percentage_Value'] = df['Value'] / 100  # Assuming your values are in percentage format already

    # Determine the y-axis range dynamically
    min_y = min(df['Percentage_Value'].min(), -0.02)
    max_y = max(df['Percentage_Value'].max(), 0.05)

    # Create the plot
    fig = go.Figure()
    

    # Create a bar plot and add data labels
    fig.add_trace(go.Bar(
        x=df['Date'],  # x-axis as Date
        y=df['Percentage_Value'],  # Use the percentage values for the y-axis
        text=[f"{val*100:,.2f}%" for val in df['Percentage_Value']],  # Display the value with decimals and percentage symbol
        textposition='outside',  # Position the text labels automatically (inside or outside the bar)
        textfont=dict(color=['white' for val in df['Percentage_Value']], size=10),  # Set the color of the labels
        cliponaxis=False,  # Ensure labels are not clipped for small/zero values
        marker=dict(
            color=['#850000' if val >= 0 else '#000085' for val in df['Percentage_Value']]
        ),
        name='Actual %'  # Set legend label for bar graph
    ))

    # Add the line plot for the 3% reference
    fig.add_trace(go.Scatter(
        x=[df['Date'].min(), df['Date'].max()],  # Use full date range for the line
        y=[0.03, 0.03],  # Horizontal line at 3%
        mode='lines+text',
        name='Projected %',
        line=dict(color='#c2a94f', width=3),
        text=['3%', None],  # Label only the starting point
        textposition='top left',
        textfont=dict(color='#c2a94f', size=12)
    ))

    # Set the title and axis labels dynamically
    fig.update_layout(
        plot_bgcolor='black',
        paper_bgcolor='black',
        font=dict(color='#c2a94f'),
        title={
            'text': title,  # Updated graph title
            'font': {'size': 14, 'color': '#c2a94f','family': 'Roboto'},
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'automargin': True,
            'pad': {'t': 10}
        },
        xaxis=dict(
            title=x_label,  # Dynamic x-axis title
            title_font=dict(family='Roboto', color='#c2a94f', size=10),
            tickfont=dict(family='Roboto', color='#c2a94f', size=10),
            showgrid=True,
            zeroline=False,
            gridcolor='rgba(255,255,255,0.2)',
            showline=True,
            tickmode='array',
            tickvals=df['Date'],  # Explicitly use the dates for ticks
            ticktext=[date.strftime('%b %d, %Y') for date in df['Date']],  # Format date as Month Day
        ),
        yaxis=dict(
            title=y_label,  # Dynamic y-axis title
            title_font=dict(family='Roboto', color='#c2a94f', size=10),
            tickfont=dict(family='Roboto', color='#c2a94f', size=10),
            showgrid=True,
            zeroline=False,
            gridcolor='rgba(255,255,255,0.2)',
            showline=True,
            tickformat='.2%',  # Format the y-axis as percentage with decimals
            range=[min_y, max_y]
        ),
        legend=dict(
            title='',  # No title for the legend
            font=dict(color='#c2a94f', size=10),
            orientation='h',  # Vertical orientation
            x=0.5,  # Center the legend horizontally at the top
            xanchor='center',
            y=0.8,  # Position the legend at the top of the chart
            yanchor='bottom'  # Align the bottom of the legend with the top of the chart
        ),
        margin=dict(l=10, r=10, t=30, b=40),  # Adjusted for better spacing
        bargap=0.2,  # Adjust the gap between bars
        bargroupgap=0.1  # Adjust the gap between groups of bars
    )


    return fig.to_json()

#====================================================================================================================================================================
# Followers Comparison 2024-2025
#====================================================================================================================================================================

def merge_platform_data(platform_results):
    merged_results = {}

    for platform, results in platform_results.items():
        # Extract the two date sets for this platform
        set_1_df = results.get("Set_1")
        set_2_df = results.get("Set_2")

        if set_1_df is not None and set_2_df is not None:
            # Ensure both dataframes have the same length by filling missing values if necessary
            max_len = max(len(set_1_df), len(set_2_df))

            # Reindex to ensure both sets are the same length, filling NaN if needed
            set_1_df = set_1_df.reindex(range(max_len))
            set_2_df = set_2_df.reindex(range(max_len))

            # Merge the two sets side by side
            merged_df = pd.DataFrame({
                "Platform": [platform] * max_len,  # Add platform column explicitly
                "Date1": set_1_df["Date"],
                "Value1": set_1_df["Value"],
                "Date2": set_2_df["Date"],
                "Value2": set_2_df["Value"]
            })

            merged_results[platform] = merged_df

    return merged_results



def followers_comparison_2024_2025(preApha=None, postApha=None, Apha=None):
    # Retrieve platform details
    platform_details = get_all_platform_details()
    
    # Fetch the date ranges or specific dates based on the input arguments
    date_or_ranges = get_date_or_range(preApha, postApha, Apha)

    if not isinstance(date_or_ranges, tuple):
        raise ValueError("Expected a tuple of date ranges, got: {}".format(date_or_ranges))

    # Split the returned date ranges into two separate lists
    date_sets = []
    for date_range in date_or_ranges:
        if "to" in date_range:
            start_date, end_date = date_range.split(" to ")
            date_list = pd.date_range(start=start_date.strip(), end=end_date.strip()).strftime('%Y-%m-%d').tolist()
        else:
            date_list = [date_range.strip()]
        date_sets.append(date_list)

    print("Date sets considered are:", date_sets)

    # Initialize result containers for each platform and date set
    results = []

    for platform, details in platform_details.items():
        platform_results = {}

        for i, date_list in enumerate(date_sets, start=1):
            platform_data = []

            try:
                connection = mysql.connector.connect(
                    host="45.76.160.28",
                    user="Summer",
                    password="Dragon",
                    database=details["db"]
                )
                cursor = connection.cursor(dictionary=True)

                for date in date_list:
                    # Initial query with time condition
                    if platform.lower() == "youtube":
                        query = f"""
                            SELECT {details['date_column']}, {details['column']}
                            FROM {details['table']}
                            WHERE {details['condition']}
                            AND DATE({details['date_column']}) = '{date}'
                            ORDER BY {details['date_column']} DESC
                            LIMIT 1;
                        """
                    else:
                        query = f"""
                            SELECT 
                                DATE_SUB({details['date_column']}, INTERVAL 1 DAY) as displayed_date,
                                {details['column']}
                            FROM {details['table']}
                            WHERE {details['condition']}
                            AND DATE({details['date_column']}) = DATE_ADD('{date}', INTERVAL 1 DAY)
                            AND TIME(created_on) BETWEEN '11:00:00' AND '11:30:00';
                        """

                    cursor.execute(query)
                    result = cursor.fetchone()
                    # print(query)

                    # Fallback query without time condition if no result found
                    if not result and platform.lower() != "youtube":
                        fallback_query = f"""
                            SELECT {details['date_column']}, {details['column']}
                            FROM {details['table']}
                            WHERE {details['condition']}
                            AND DATE({details['date_column']}) = '{date}'
                            ORDER BY {details['date_column']} DESC
                            LIMIT 1;
                        """
                        cursor.execute(fallback_query)
                        result = cursor.fetchone()
                        # print(query)

                    platform_data.append({
                        "Platform": platform,
                        "Date": date,
                        "Value": result[details['column']] if result else None
                    })

                platform_results[f"Set_{i}"] = pd.DataFrame(platform_data)

            except mysql.connector.Error as err:
                print(f"Database error for {platform}: {err}")
            finally:
                if cursor:
                    cursor.close()
                if connection:
                    connection.close()



        # Store results for this platform
        results_dict = merge_platform_data({platform: platform_results})
        final_df = pd.concat(results_dict.values(), ignore_index=True)
        print(final_df)

        # Call the plotting function with the merged dataframe
        graphs_json = plot_graph_followers_comparison_2024_2025(final_df)

        difference = final_df['Value1'].iloc[0] - final_df['Value2'].iloc[0]

        # Generate commentary
        commentary = []
        commentary.append(f"The graph shows the comparison of the followers count for 2025 and 2024 which correspond to the same day of the event for both the years.")
        commentary.append(f"The Difference in the followers count is {difference:,}")


        # Append results for this platform
        results.append({
            'platform': platform,
            'graphs_json': graphs_json,
            'commentary': commentary
        })

    return results



def plot_graph_followers_comparison_2024_2025(df):
    unique_platforms = df['Platform'].unique()

    # Iterate through each platform and add traces for each
    for platform in unique_platforms:
        platform_df = df[df['Platform'] == platform]

        # Format dates to "Month Day, Year" (e.g., May 13, 2024)
        formatted_date1 = pd.to_datetime(platform_df['Date1']).dt.strftime('%b %d, %Y')
        formatted_date2 = pd.to_datetime(platform_df['Date2']).dt.strftime('%b %d, %Y')


        # Create custom labels for X-axis using Date1 and Date2
        x_labels = [f"{d1}, {d2}" for d1, d2 in zip(formatted_date1, formatted_date2)]
        value1 = platform_df['Value1'].tolist()  # Gold bars
        value2 = platform_df['Value2'].tolist()  # Maroon bars

        # Calculate the maximum value from both data sets
        max_value = max(max(value1, default=0), max(value2, default=0))

        # Add a buffer (e.g., 10%) to ensure the y-axis scale is slightly above the maximum value
        y_axis_max = max_value * 1.1

        # Determine the title dynamically based on the platform
        if platform.lower() == "youtube":
            title_text = f"APHA 2025 v/s APHA 2024 {platform} Subscribers Comparison"
        else:
            title_text = f"APHA 2025 v/s APHA 2024 {platform} Followers Comparison"

        # Create the figure for the platform
        fig = go.Figure()

        # Add gold bars for Value1 (for each platform)
        fig.add_trace(go.Bar(
            x=x_labels,  # Gold bar labels
            y=value1,
            name='2025',
            marker=dict(color='#c2a94f'),  # Gold color
            text=[f"{v:,.0f}" for v in value1],  # Format data labels with commas
            textposition='inside',  # Display labels inside bars
            textfont=dict(size=10, color='black')  # Set data label font size
        ))

        # Add maroon bars for Value2 (for each platform)
        fig.add_trace(go.Bar(
            x=x_labels,  # Maroon bar labels
            y=value2,
            name='2024',
            marker=dict(color='#850000'),  # Maroon color
            text=[f"{v:,.0f}" for v in value2],  # Format data labels with commas
            textposition='inside',  # Display labels inside bars
            textfont=dict(size=10, color='white')  # Set data label font size
        ))

        # Update layout for grouped bars
        fig.update_layout(
            title={
                'text': title_text,
                'x': 0.5,  # Center-align the title
                'y': 0.95,  # Position slightly above the graph area
                'xanchor': 'center',
                'yanchor': 'top',
                'font': {'size': 14, 'color': '#c2a94f'},
            },
            xaxis=dict(
                title="Dates",  # Dynamic x-axis title
                title_font=dict(family='Roboto', color='#c2a94f', size=10),
                tickfont=dict(family='Roboto', color='#c2a94f', size=10),
                showgrid=True,
                zeroline=False,
                gridcolor='rgba(255,255,255,0.2)',
                showline=True,
                tickmode='array',
                tickvals=x_labels,
                ticktext=x_labels,  # Show formatted dates as individual labels
                automargin=True,
                title_standoff=20  # Add padding between the title and labels
            ),
            yaxis=dict(
                title="Follower/Subscribers Count",  # Dynamic y-axis title
                title_font=dict(family='Roboto', color='#c2a94f', size=10),
                tickfont=dict(family='Roboto', color='#c2a94f', size=10),
                showgrid=True,
                zeroline=False,
                gridcolor='rgba(255,255,255,0.2)',
                showline=True,
                tickformat=',.0f',
                range=[0, y_axis_max],  # Set y-axis range dynamically
            ),
            barmode='group',  # Grouped bars
            plot_bgcolor='black',
            paper_bgcolor='black',
            font=dict(color='#c2a94f'),
            legend=dict(title='Legend', font=dict(size=10),
                        orientation='h',
                        x=0.5,  # Center the legend horizontally at the top
                        xanchor='center',
                        y=1,  # Position the legend at the top of the chart
                        yanchor='bottom'  # Align the bottom of the legend with the top of the chart
                    ),

            margin=dict(l=40, r=20, t=70, b=70)  # Reduce right-side space
        )

        
    # Return the list of graphs for all platforms
    return fig.to_json()


#========================================================================================================================================================================
# function to plot the Articles views graph for APHA website
#========================================================================================================================================================================

def apha_2025_Articles_Analysis_Graph(preApha=None, postApha=None, Apha=None):
    date_or_range = get_date_or_range(preApha, postApha, Apha)

    # If the return value is a tuple (like in the APHA case), unpack it
    if isinstance(date_or_range, tuple):
        # Consider only the first entry in the tuple
        date_or_range = date_or_range[0]

    if "to" in date_or_range:
        start_date, end_date = date_or_range.split(" to ")
        date_list = pd.date_range(start=start_date.strip(), end=end_date.strip()).strftime('%Y-%m-%d').tolist()
    else:
        # Single date input
        start_date = end_date = date_or_range.strip()
        date_list = [date_or_range.strip()]

    connection = None
    cursor = None

    try:
        # Establish the database connection
        connection = mysql.connector.connect(
            host="207.148.79.97",
            user="autumn",
            password="dragon12@",
            database="article_management_service"
        )
        
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            # Ensure date format matches DB requirements
            date_list = [pd.to_datetime(date).strftime('%Y-%m-%d') for date in date_list]
            print("Date List:", date_list)
            
            # Debug the query and parameters
            placeholders = ', '.join(['%s'] * len(date_list))
            query = f"""
                SELECT DISTINCT a.articleID, a.postedBy, 
                        COALESCE(v.viewcount, 0) AS viewcount
                FROM article_management_service.article a
                INNER JOIN article_management_service.ArticleWebsite c ON a.articleID = c.articleID
                LEFT JOIN article_management_service.viewcountgroup v ON a.articleID = v.articleID
                WHERE c.website_id = 14 and date(a.postedBy) in ({placeholders})                
            """

            # Execute the query
            cursor.execute(query, date_list)
            apha_articles_data = cursor.fetchall()
            
            if not apha_articles_data:
                print(f"No data found for the date range {start_date} to {end_date}")
                return None

            # Convert the fetched data to a DataFrame
            df = pd.DataFrame(apha_articles_data)
            
            if df.empty:
                print(f"No data found for the date range {start_date} to {end_date}")
                return None
            
            # Sum view counts by 'postedBy' and prepare data for plotting
            df1 = df.groupby('postedBy', as_index=False)['viewcount'].sum()
            df1['visitorsDate'] = pd.to_datetime(df1['postedBy']).dt.date
            df1['pageViews'] = df1['viewcount'].astype(int)
            
            y_max = df1['pageViews'].max()

            # Create a bar plot using Plotly
            fig = go.Figure(
                data=[go.Bar(
                    x=df1['visitorsDate'], 
                    y=df1['pageViews'],
                    text=df1['pageViews'],  # Data labels for all bars
                    textposition='outside',  # Position the labels outside the bars
                    marker_color='#850000',
                    textfont=dict(size=10)
                )]
            )

            fig.update_layout(
                title={
                    #'text': f'APHA Total Articles Views {start_date} to {end_date}',
                    'x': 0.5,
                    'xanchor': 'center',
                    'font': {'size': 12}
                },
                xaxis=dict(
                    title='Date',
                    tickmode='array',  # Explicitly set all tick values
                    tickvals=df1['visitorsDate'],  # All dates as ticks
                    ticktext=[d.strftime('%b %d, %Y') for d in pd.to_datetime(df1['visitorsDate'])],  # Custom labels with year
                    tickangle=30,  # Tilt x-axis labels
                    showgrid=False,
                    linecolor='white',
                    title_font=dict(family='Roboto', color='#c2a94f', size=10),
                    tickfont=dict(family='Roboto', color='#c2a94f', size=10)
                ),
                yaxis=dict(
                    title='Articles Views Count',
                    showgrid=False,
                    linecolor='white',
                    range=[0, y_max + y_max * 0.1],
                    title_font=dict(family='Roboto', color='#c2a94f', size=10),
                    tickfont=dict(family='Roboto', color='#c2a94f', size=10)
                ),
                plot_bgcolor='black',
                paper_bgcolor='black',
                font=dict(color='#c2a94f'),
                margin=dict(l=10, r=10, t=30, b=10)
            )
            
            # output_filename = f"articles_count.jpeg"
            # fig.write_image(output_filename)
            # fig.show()

            # Generate commentary
            if "to" in str(date_or_range):
                commentary = [
                    f"The bar graph shows the overall views of all articles posted on the Health 72 website between {date_or_range}."
                ]
            else:
                commentary = [
                    f"The bar graph shows the overall views of all articles posted on the Health 72 website on {date_or_range}."
                ]            
            results = {}
            results['graph_json'] = fig.to_json()
            results['commentary'] = commentary
            
            # Return the plot as a JSON object with commentary
            return results
        else:
            print("Connection to the database failed.")
            return None

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()
        else:
            print("Connection to the database was not established.")

#======================================================================================================================================================================
# Function to get database connection and table details
#======================================================================================================================================================================

def get_db_connection_and_table(platform):
    if platform == "Facebook":
        database = "social_media_fb"
        table_name = "post_insights"
    elif platform == "Instagram":
        database = "social_media_insta"
        table_name = "caption_comments_count"
    elif platform == "YouTube":
        database = "youtube"
        table_name = "youtubeVideoMetricsHealth"
    else:
        return []

    # Establish the connection
    connection = mysql.connector.connect(
        host="45.76.160.28",
        user="Summer",
        password="Dragon",
        database=database
    )

    return connection, table_name, database
#========================================================================================================================================================================
#Create commentary
#========================================================================================================================================================================
def create_commentary(posts_df, start_date, end_date=None,platform = "Facebook"):
    if posts_df.empty:
        return [f"No posts are available for {platform.title()} on {start_date}."]

    commentary = []

    if end_date is None:  # Single date
        commentary.append(f"The above table displays post insights for {platform.title()} on {start_date}.")
        
    else:  # Date range
        commentary.append(f"The above table displays post insights for {platform.title()} from {start_date} to {end_date}.")
    
    # Add total number of posts
    commentary.append(f"Total number of posts published on {platform.title()}: {len(posts_df)}")
    
    return commentary
#========================================================================================================================================================================
#POST INSIGHTS TABLE - FACEBOOK
#========================================================================================================================================================================
def get_selected_columns_in_date_range_FBPost(preApha=False, postApha=False, Apha=None):
    
    platform = "Facebook"
    facebook_commentary = ""
    json_top_posts = ""
    
    results = {}  # Initialize results dictionary to append responses

    # Get the specific date or range using get_date_or_range
    date_range = get_date_or_range(preApha, postApha, Apha)

    # Initialize variables
    start_date, end_date = None, None

    # Handle the case when a list or tuple is returned
    if isinstance(date_range, tuple):  # For preAPHA or postAPHA
        if " to " in date_range[0]:  # Check if " to " exists
            start_date, end_date = date_range[0].split(" to ")
        else:
            start_date = date_range[0]
            end_date = None
    elif isinstance(date_range, list):  # For APHA-specific dates
        start_date = date_range[0]
        end_date = None

    # Convert input_date to a datetime object to check the day of the week
    if isinstance(start_date, str):  # Ensure start_date is a string
        date_range_obj = datetime.strptime(start_date, '%Y-%m-%d')
        day_of_week = date_range_obj.strftime('%A')

    # Get connection and table details
    connection, _, _ = get_db_connection_and_table(platform)
    if connection is None:
        results["error"] = "Connection failed"
        return results

    try:
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            # Define the query
            query = """
                SELECT pi.channel_id, pi.data_id, pi.message, pi.full_picture, pi.likes_count, pi.comments_count, pi.reach_count, pi.impressions_count, pi.add_time, c.channel_name, c.token
                FROM post_insights pi
                JOIN channels c ON pi.channel_id = c.channel_id
            """

            if end_date is None:  # Single date
                print(f"Fetching data for: {start_date}")
                query += """ WHERE DATE(pi.add_time) = %s AND pi.channel_id = 2 """
                cursor.execute(query, (start_date,))
            else:  # Date range
                query += """ WHERE DATE(pi.add_time) BETWEEN %s AND %s AND pi.channel_id = 2 """
                cursor.execute(query, (start_date, end_date))

            # Fetch all results
            posts = cursor.fetchall()
            hashtag_fb_df = pd.DataFrame(posts)

            # Check if posts exist
            if posts:
                # Convert to DataFrame
                df = pd.DataFrame(posts, columns=['channel_id', 'data_id', 'message', 'full_picture', 'likes_count', 'comments_count', 'reach_count', 'impressions_count', 'add_time','token'])

                # Process 'message' column
                if 'message' in df.columns:
                    df['message'] = df['message'].str.split('#|https').str[0].str.strip()

                # Process 'full_picture' column
                if 'full_picture' in df.columns:
                    df['valid_media_url'] = df['full_picture']
                    df['post_url'] = None  # New column for storing post_url
                    valid_rows = []
                    for i in range(len(df)):
                        img_url = df.at[i, 'full_picture']
                        post_id = df.at[i, 'data_id']
                        access_token = df.at[i, 'token']
                        try:
                            if img_url:  # Check if img_url is not empty
                                response = requests.head(img_url)
                                if response.status_code == 200:
                                    valid_rows.append(i)
                                else:    
                                    new_img_url = f"https://graph.facebook.com/v14.0/{post_id}?fields=attachments{{media}}&access_token={access_token}"
                                    response_img = requests.get(new_img_url)

                                    # Handle responses
                                    if response_img.status_code == 200:
                                        try:
                                            df.at[i, 'valid_media_url'] = response_img.json()['attachments']['data'][0]['media']['image']['src']
                                            valid_rows.append(i)
                                        except (KeyError, IndexError) as e:
                                            print(f"Error processing media URL for post {post_id}: {e}")

                            else:
                                print(f"Invalid or empty img_url at index {i}: {img_url}")
                        except requests.exceptions.RequestException as e:
                            print(f"Error fetching {img_url} at index {i}: {e}")
                    
                        
                        # New logic for post_url
                        post_url = f"https://graph.facebook.com/v14.0/{post_id}?fields=attachments&access_token={access_token}"
                        response_post = requests.get(post_url)

                        if response_post.status_code == 200:
                            try:
                                df.at[i, 'post_url'] = response_post.json()['attachments']['data'][-1]['url']
                            except (KeyError, IndexError) as e:
                                print(f"Error fetching post URL for post {post_id}: {e}")

                    df = df.loc[valid_rows]

                # Drop rows where message is empty or blank
                df['message'].replace('', pd.NA, inplace=True)
                df = df.dropna(subset=['message'])

                # Remove rows where all metrics are 0
                metrics = ['reach_count', 'impressions_count', 'likes_count', 'comments_count']
                df = df[~(df[metrics].sum(axis=1) == 0)]

                # Sort by impressions_count in descending order
                sorted_df = df.sort_values(by=['impressions_count'], ascending=False)

                # Reorder columns
                columns_order = ['valid_media_url', 'reach_count', 'impressions_count', 'likes_count', 'comments_count', 'message', 'post_url']
                top_posts = sorted_df[columns_order]

                # to add commas for nos greater than 100
                top_posts[['reach_count', 'impressions_count', 'likes_count']] = top_posts[['reach_count', 'impressions_count', 'likes_count']].apply(lambda x: x.apply(lambda y: "{:,}".format(int(y)) if str(y).replace(',', '').replace('.', '').isdigit() else y))

                # Convert DataFrame to JSON
                json_top_posts = top_posts.to_json(orient='records')

                # Use the create_commentary function to generate the commentary
                facebook_commentary = create_commentary(top_posts, start_date, end_date,platform = "Facebook")
                results["top_posts"] = json_top_posts
                results["commentary"] = facebook_commentary

            else:
                # No posts fetched
                if day_of_week in ['Saturday', 'Sunday']:
                    facebook_commentary = [f"No posts are available for Facebook on {start_date} due to weekends and holidays."]
                else:
                    facebook_commentary = [f"No posts are available for Facebook on {start_date}."]
                results["top_posts"] = 'None'
                results["commentary"] = facebook_commentary
                return results

        else:
            results["error"] = "Connection failed"
            return results
        
        # print("results : ",results)

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        results["error"] = str(err)
        return results
    finally:
        cursor.close()
        connection.close()

    return results, hashtag_fb_df

#========================================================================================================================================================================
#POST INSIGHTS TABLE - INSTAGRAM
#========================================================================================================================================================================
def get_selected_columns_in_date_range_InstaPost(preApha=False, postApha=False, Apha=None):

    platform = "Instagram"
    Instagram_commentary = ""
    json_top_posts = ""
    results = {}  # Initialize results dictionary

    # Get the specific date or range using get_date_or_range
    date_range = get_date_or_range(preApha, postApha, Apha)

    # Initialize variables
    start_date, end_date = None, None

    # Handle the case when a list or tuple is returned
    if isinstance(date_range, tuple):  # For preAPHA or postAPHA
        if " to " in date_range[0]:  # Check if " to " exists
            start_date, end_date = date_range[0].split(" to ")
        else:
            start_date = date_range[0]
            end_date = None
    elif isinstance(date_range, list):  # For APHA-specific dates
        start_date = date_range[0]
        end_date = None

    # Convert input_date to a datetime object to check the day of the week
    if isinstance(start_date, str):  # Ensure start_date is a string
        date_range_obj = datetime.strptime(start_date, '%Y-%m-%d')
        day_of_week = date_range_obj.strftime('%A')

    # Get connection and table details
    connection, _, _ = get_db_connection_and_table(platform)
    if connection is None:
        results["connection"] = "Database connection failed"
        return results

    try:
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
        
        # Define the query using LEFT JOIN to include all posts, even if impressions or reach are missing
        query = """
            SELECT p.data_id, p.channel_id, p.add_time, p.caption, p.media_url, p.like_count, p.comments_count,
            COALESCE(ir.impression_count, 'NA') AS impression_count,
            COALESCE(ir.reach_count, 'NA') AS reach_count, c.channel_id, c.channel_name, c.channel_unique_id, c.token 
            FROM caption_comments_count p 
            LEFT JOIN channels c ON p.channel_id = c.channel_id 
            LEFT JOIN impression_reach_count ir ON p.channel_id = ir.channel_id AND p.data_id = ir.data_id 
        """

        if end_date is None:  # If it's a single date (not a range)
            results["date_info"] = f"Fetching data for: {start_date}"
            query += """ WHERE DATE(p.add_time) = %s AND p.channel_id = 7 """
            cursor.execute(query, (start_date,))
        else:  # If it's a date range
            results["date_info"] = f"Fetching data between: {start_date} and {end_date}"
            query += """ WHERE DATE(p.add_time) BETWEEN %s AND %s AND p.channel_id = 7 """
            cursor.execute(query, (start_date, end_date))

        # Fetch all results
        posts = cursor.fetchall()
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(posts)
        hashtag_insta_df = pd.DataFrame(posts)

        df['post_url']=''

        # Append URLs to img_urls
        img_urls = df['media_url'].tolist()
        
        # Update caption column to remove text after '#' and 'https'
        if 'caption' in df.columns:
            df['caption'] = df['caption'].str.split('#|https').str[0].str.strip()

        # Ensure 'reach_count' is numeric for comparison
        df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce')

        # Group by caption and select the row with the highest reach_count
        df = df.loc[df.groupby('caption')['like_count'].idxmax()]
        
        # Create a new column for valid media URLs
        df['valid_media_url'] = df['media_url']
        
        # Check each media URL and update if it's a video or if the URL is broken
        for i, row in df.iterrows():
            img_url = row['media_url']
            post_id = row['data_id']
            access_token = row['token']
            post_url = f"https://graph.facebook.com/v13.0/{post_id}?fields=caption,comments_count,like_count,media_url,timestamp,permalink&access_token={access_token}"
            response = requests.get(post_url)
            df.at[i, 'post_url'] = response.json().get('permalink', '')
            
            if ".mp4" in img_url:
                # Fetch the thumbnail URL for the video
                new_img_url = f"https://graph.facebook.com/v13.0/{post_id}?fields=thumbnail_url&access_token={access_token}"
                response = requests.get(new_img_url)
                if response.status_code == 200:
                    df.at[i, 'media_url'] = response.json().get('thumbnail_url', img_url)
            else:
                response = requests.head(img_url)
                if response.status_code != 200:
                    # If the URL is broken, try to get a new one
                    new_img_url = f"https://graph.facebook.com/v13.0/{post_id}?fields=caption,comments_count,like_count,media_url,timestamp&access_token={access_token}"
                    response = requests.get(new_img_url)
                    if response.status_code == 200:
                        df.at[i, 'valid_media_url'] = response.json().get('media_url', img_url)

        
        # Check for expired media_url and use valid_media_url as fallback
        for i, row in df.iterrows():
            media_url = row['media_url']
            if media_url:  # Check if media_url is not empty or None
                response = requests.head(media_url)
                if response.status_code != 200:  # If media_url is expired or inaccessible
                    df.at[i, 'media_url'] = row['valid_media_url']  # Use valid_media_url as fallback

        
        # Check for 'NA' in reach_count and fetch from the Facebook API
        for i, row in df.iterrows():
            if row['reach_count'] == 'NA':
                post_id = row['data_id']
                access_token = row['token']
                reach_url = f"https://graph.facebook.com/v7.0/{post_id}/insights?access_token={access_token}&metric=reach&period=day"
                
                response = requests.get(reach_url)
                
                if response.status_code == 200:
                    reach_data = response.json()
                    
                    # Check if valid data is returned
                    if 'data' in reach_data and len(reach_data['data']) > 0:
                        if 'values' in reach_data['data'][0] and len(reach_data['data'][0]['values']) > 0:
                            df.at[i, 'reach_count'] = reach_data['data'][0]['values'][0]['value']
                        else:
                            # If no value is found, keep 'NA'
                            df.at[i, 'reach_count'] = 'NA'
                    else:
                        # If the API returns no data, keep 'NA'
                        df.at[i, 'reach_count'] = 'NA'
                else:
                    # If the API request fails, keep 'NA'
                    df.at[i, 'reach_count'] = 'NA'
                    
        # Check for 'NA' in impression_count and fetch from the Facebook API
        for i, row in df.iterrows():
            if row['impression_count'] == 'NA':
                post_id = row['data_id']
                access_token = row['token']
                impression_url = f"https://graph.facebook.com/v7.0/{post_id}/insights?access_token={access_token}&metric=impressions&period=day"
                
                response = requests.get(impression_url)
                
                if response.status_code == 200:
                    impression_data = response.json()
                    
                    # Check if valid data is returned
                    if 'data' in impression_data and len(impression_data['data']) > 0:
                        if 'values' in impression_data['data'][0] and len(impression_data['data'][0]['values']) > 0:
                            df.at[i, 'impression_count'] = impression_data['data'][0]['values'][0]['value']
                        else:
                            # If no value is found, keep 'NA'
                            df.at[i, 'impression_count'] = 'NA'
                    else:
                        # If the API returns no data, keep 'NA'
                        df.at[i, 'impression_count'] = 'NA'
                else:
                    # If the API request fails, keep 'NA'
                    df.at[i, 'impression_count'] = 'NA'
                    
        # Sort by impression_count in descending order
        sorted_df = df.sort_values(by=['impression_count'], ascending=[False])

        # Group by channel and get the top 5 posts for each channel
        top_posts = sorted_df.reset_index(drop=True)

        if top_posts.empty:
            if day_of_week in ['Saturday', 'Sunday']:
                Instagram_commentary = [f"No posts are available for Instagram on {start_date} due to weekends and holidays."]
            else:
                Instagram_commentary = [f"No posts are available for Instagram on {start_date}."]
            results["top_posts"] = 'None'
            results["commentary"] = Instagram_commentary
            return results
        
        # Create commentary message for Instagram available data in commentary box
        Instagram_commentary = create_commentary(top_posts, start_date, end_date,platform = "Instagram")
        results["commentary"] = Instagram_commentary

        # Reorder columns as specified
        columns_order = ['media_url', 'reach_count', 'impression_count', 'like_count', 'comments_count', 'caption', 'post_url']
        top_posts = top_posts[columns_order]

        # to add commas for nos greater than 100
        top_posts[['reach_count', 'impression_count', 'like_count']] = top_posts[['reach_count', 'impression_count', 'like_count']].apply(lambda x: x.apply(lambda y: "{:,}".format(int(y)) if str(y).replace(',', '').replace('.', '').isdigit() else y))
        
        json_top_posts = top_posts.to_json(orient='records')
        results["top_posts"] = json_top_posts

        # print(results)

    except mysql.connector.Error as err:
        results["error"] = f"Database error: {err}"
    finally:
        cursor.close()
        connection.close()

    return results, hashtag_insta_df

#========================================================================================================================================================================
# POST INSIGHTS TABLE - YOUTUBE 
#========================================================================================================================================================================

def get_selected_YoutubeVideosHealth(preApha=False, postApha=False, Apha=None):

    platform = "YouTube"
    youtube_health_commentary = ""
    results = {}  # Initialize results dictionary
    json_top_posts = ""
    sorted_df = pd.DataFrame()
    top_posts = pd.DataFrame()

    # Get the specific date or range using get_date_or_range
    date_range = get_date_or_range(preApha, postApha, Apha)

    # Initialize variables
    start_date, end_date = None, None

    # Handle the case when a list or tuple is returned
    if isinstance(date_range, tuple):  # For preAPHA or postAPHA
        if " to " in date_range[0]:  # Check if " to " exists
            start_date, end_date = date_range[0].split(" to ")
        else:
            start_date = date_range[0]
            end_date = None
    elif isinstance(date_range, list):  # For APHA-specific dates
        start_date = date_range[0]
        end_date = None

    # Convert input_date to a datetime object to check the day of the week
    if isinstance(start_date, str):  # Ensure start_date is a string
        date_range_obj = datetime.strptime(start_date, '%Y-%m-%d')
        day_of_week = date_range_obj.strftime('%A')

    # Get connection and table details
    connection, _, _ = get_db_connection_and_table(platform)
    if connection is None:
        results["connection"] = "Database connection failed"
        return results

    try:
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

        # Define the query
        query = """
        SELECT CreatedOn, video_id, title, published_at, viewCount, likeCount, commentCount
        FROM youtubeVideoMetricsHealth
        """

        if end_date is None:  # If it's a single date (not a range)
            results["date_info"] = f"Fetching data for: {start_date}"
            query += """ WHERE DATE(published_at) = %s """
            cursor.execute(query, (start_date,))
        else:  # If it's a date range
            results["date_info"] = f"Fetching data between: {start_date} and {end_date}"
            query += """ WHERE DATE(published_at) BETWEEN %s AND %s  """
            cursor.execute(query, (start_date, end_date))

        # Fetch all results
        posts = cursor.fetchall()

        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(posts)

        # Filter the DataFrame to keep only the most recent 'CreatedOn' values for each 'video_id'
        if 'CreatedOn' in df.columns:
            df['CreatedOn'] = pd.to_datetime(df['CreatedOn'])
            idx = df.groupby(['video_id'])['CreatedOn'].idxmax()
            df = df.loc[idx]

        # Add new columns for video URL and thumbnail image URL
        if 'video_id' in df.columns:
            df['video_url'] = df['video_id'].apply(lambda x: f"https://www.youtube.com/watch?v={x}")
            df['thumbnail_url'] = df['video_id'].apply(lambda x: f"https://img.youtube.com/vi/{x}/0.jpg")

        # Display images in DataFrame
        if 'thumbnail_url' in df.columns:
            df['thumbnail_img'] = df['thumbnail_url'].apply(lambda x: f'<img src="{x}" width="100" height="100">')

        # Sort by viewCount in descending order
        if 'viewCount' in df.columns:
            sorted_df = df.sort_values(by=['viewCount'], ascending=False)

            # Ensure sorted_df is a DataFrame before calling reset_index
            if isinstance(sorted_df, pd.DataFrame):
                # Reset index after sorting
                top_posts = sorted_df.reset_index(drop=True)

                # Reorder columns as specified
                columns_order = ['thumbnail_url', 'viewCount', 'likeCount', 'commentCount', 'title', 'video_url']
                top_posts = top_posts[columns_order]

                # to add commas for nos greater than 100
                top_posts[['viewCount', 'likeCount']] = top_posts[['viewCount', 'likeCount']].apply(lambda x: x.apply(lambda y: "{:,}".format(int(y)) if str(y).replace(',', '').replace('.', '').isdigit() else y))


        if top_posts.empty:
            if day_of_week in ['Saturday', 'Sunday']:
                youtube_health_commentary = [f"No videos posted are available for YouTube on {start_date} due to weekends and holidays."]
            else:
                youtube_health_commentary = [f"No videos posted are available for YouTube on {start_date}."]
            results["top_posts"] = 'None'
            results["commentary"] = youtube_health_commentary
            return results

        # Create commentary message for available data
        youtube_health_commentary = create_commentary(top_posts, start_date, end_date,platform = "Youtube")
        results["commentary"] = youtube_health_commentary

        # Convert DataFrame to JSON
        json_top_posts = top_posts.to_json(orient='records')
        results["top_posts"] = json_top_posts

    except mysql.connector.Error as err:
        results["error"] = f"Database error: {err}"
    finally:
        cursor.close()
        connection.close()

    return results




#====================================================================================================================================================================
# ARTICLE ANALYTICS TABLE
#====================================================================================================================================================================
def apha_Articles_Analysis(preApha=False, postApha=False, Apha=None):
    json_apha_Articles = ""

    # Get the specific date or range using get_date_or_range
    date_range = get_date_or_range(preApha, postApha, Apha)

    # Handle the case when a list or tuple is returned
    if isinstance(date_range, tuple):  # For preAPHA or postAPHA
        date_range = date_range[0]  # Use only the first range
    elif isinstance(date_range, list):  # For APHA-specific dates
        date_range = date_range[0]  # Use only the first date

    print(date_range)

    try:
        # Establish a single database connection
        connection = mysql.connector.connect(
            host="207.148.79.97",
            user="autumn",
            password="dragon12@",
            database="article_management_service"
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            # Base query with website filtering
            query = """
                SELECT DISTINCT a.articleID, 
                            a.articleTitle, 
                            a.articleSlug, 
                            a.articleThumbnail, 
                            a.articleType, 
                            a.postedBy, 
                            COALESCE(v.viewcount, 'NA') AS viewcount
                        FROM article_management_service.article a
                        INNER JOIN article_management_service.ArticleWebsite c ON a.articleID = c.articleID
                        LEFT JOIN article_management_service.viewcountgroup v ON a.articleID = v.articleID
                        WHERE c.website_id = 14
            """
            
            # Prepare parameters for date filtering
            query_params = []

            # Determine date filtering condition
            if "to" not in str(date_range):  # Single date
                print(f"Fetching data for single date: {date_range}")
                query += " AND DATE(a.postedBy) = %s"
                query_params.append(date_range)
            else:
                # Date range
                start_date, end_date = date_range.split(" to ")
                query += " AND DATE(a.postedBy) BETWEEN %s AND %s"
                query_params.extend([start_date, end_date])
                print(f"Fetching data from {start_date} to {end_date}")

            # Execute the query with date parameters
            cursor.execute(query, tuple(query_params))

            apha_articles_data = cursor.fetchall()

            # Convert the fetched data to a DataFrame
            df = pd.DataFrame(apha_articles_data)

            if df.empty:
                print(f"No data found for {date_range}")
                return {
                    'data': '[]',
                    'commentary': [f"No articles found for the date range {date_range}"]
                }

            #print("df1 : ", df)

            # Convert 'postedBy' to date only
            df['postedBy'] = pd.to_datetime(df['postedBy'], errors='coerce').dt.date

            # Additional filter to ensure date range
            if "to" in str(date_range):
                start_date, end_date = date_range.split(" to ")
                start_date = pd.to_datetime(start_date).date()
                end_date = pd.to_datetime(end_date).date()
                
                # Explicitly filter the DataFrame
                df = df[
                    (df['postedBy'] >= start_date) & 
                    (df['postedBy'] <= end_date)
                ]

                if df.empty:
                    print(f"No data found within the specified date range {date_range}")
                    return {
                        'data': '[]',
                        'commentary': [f"No articles found within the date range {date_range}"]
                    }

            # Add URL prefix to articleThumbnail column and store in a new column
            df['Thumbnail'] = 'https://articles.72dragonscannes.com/uploads/thumbnails/' + df['articleThumbnail']
            df['article_url'] = 'https://health72.com/IndividualArticle/' + df['articleSlug']

            # Select only the desired columns
            column_order = ['Thumbnail', 'articleTitle', 'viewcount', 'article_url', 'postedBy']
            final_df = df[column_order]

            # Sort the DataFrame by 'viewcount' in descending order
            final_df['viewcount'] = pd.to_numeric(final_df['viewcount'], errors='coerce').fillna(0)
            final_df = final_df.sort_values(by='viewcount', ascending=False)


            if 'postedBy' in final_df.columns:
                final_df.loc[:, 'postedBy'] = final_df['postedBy'].astype(str)

            total_articles = len(final_df)
            if "to" in str(date_range):
                commentary = [
                    f"On 72 Dragons Health website, {total_articles} articles were posted between {date_range}."
                    # "The table includes article titles, view counts, thumbnails, and posting dates."
                ]
            else:
                commentary = [
                    f"On 72 Dragons Health website, {total_articles} articles were posted on {date_range}."
                    # "The table includes article titles, view counts, thumbnails, and posting dates."
                ]


            # Convert data to JSON for the dashboard
            json_apha_Articles = final_df.to_json(orient='records')

            # Prepare the results
            results = {
                'data': json_apha_Articles,
                'commentary': commentary
            }

            return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return {
            'data': '[]',
            'commentary': [f"Database error: {err}"]
        }

    except Exception as e:
        print(f"Unexpected error: {e}")
        return {
            'data': '[]',
            'commentary': [f"Unexpected error: {e}"]
        }

    finally:
        # Ensure connection is closed
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

    return results

#====================================================================================================================================================================
#CORE FOLLOWERS TABLE 
#====================================================================================================================================================================
# Function to fetch data for a specific date

def coreFollowers(preApha=False, postApha=False, Apha=None):
    # Get the specific date or range using get_date_or_range
    date_range = get_date_or_range(preApha, postApha, Apha)
    start_date, end_date = None, None

    # Handle the case when a tuple is returned
    if isinstance(date_range, tuple):  # For preAPHA, postAPHA, or specific APHA date
        start_date = date_range[0]
        if " to " in date_range[0]:
            start_date, end_date = date_range[0].split(" to ")

    # Establish the connection
    connection = mysql.connector.connect(
        host="45.76.160.28",
        user="Summer",
        password="Dragon",
    )

    # Get connection and table details
    if connection is None:
        return

    try:
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            query = f""" SELECT * FROM social_media_fb.apha2025_coreFollowers """

            if end_date:  # Date range
                query += " WHERE Date_of_core_follower_achieved BETWEEN %s AND %s"
                cursor.execute(query, (start_date, end_date))
            else:  # Single date
                query += " WHERE Date_of_core_follower_achieved = %s"
                cursor.execute(query, (start_date,))

            result = cursor.fetchall()

            # Convert results to a DataFrame
            df = pd.DataFrame(result)

            # Check if 'Date_of_core_follower_achieved' column exists and convert to string
            if 'Date_of_core_follower_achieved' in df.columns:
                df['Date_of_core_follower_achieved'] = df['Date_of_core_follower_achieved'].astype(str)

            # Format Instagram and Facebook follower counts
            for col in ['Instagram_Followers_Count', 'Facebook_Followers_Count']:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: "{:,}".format(int(x.replace(',', '').replace('.', '')))
                        if isinstance(x, str) and x.replace(',', '').replace('.', '').isdigit()
                        else x
                    )

            # Calculate placeholder values for the commentary
            total_core_followers = len(df)  # Placeholder 1
            highest_achieved_message = "N/A"  # Default message

            if 'AchievedBy' in df.columns and not df.empty:  # Ensure column exists and df is not empty
                achieved_counts = df['AchievedBy'].value_counts()
                
                # Check if all members have achieved only 1 core follower
                if achieved_counts.max() == 1 and (achieved_counts == 1).all():
                    all_achievers = list(achieved_counts.index)
                    
                    # Format the list of names nicely
                    if len(all_achievers) == 1:
                        highest_achieved_message = f"{all_achievers[0]} has achieved one core follower"
                    elif len(all_achievers) == 2:
                        highest_achieved_message = f"{all_achievers[0]} and {all_achievers[1]} have achieved one core follower each"
                    else:
                        # For 3+ people, use comma separation with "and" before the last one
                        highest_achieved_message = f"{', '.join(all_achievers[:-1])}, and {all_achievers[-1]} all have achieved one core follower each"
                else:
                    # Get the maximum count
                    max_count = achieved_counts.max()
                    
                    # Find all people who achieved this max count
                    top_achievers = [name for name, count in achieved_counts.items() if count == max_count]
                    
                    # Format the list of top achievers
                    if len(top_achievers) == 1:
                        highest_achieved_message = f"{top_achievers[0]}"
                    elif len(top_achievers) == 2:
                        highest_achieved_message = f"{top_achievers[0]} and {top_achievers[1]}"
                    else:
                        highest_achieved_message = f"{', '.join(top_achievers[:-1])}, and {top_achievers[-1]}"

            # Commentary
            commentary = [
                f"The total number of core followers obtained are : {total_core_followers}"
                # f"Highest core follower was achieved by: {highest_achieved_message}."
            ]

            # Convert DataFrame to JSON
            json_CoreFollowers = df.to_json(orient='records')

            # Return both JSON data and commentary
            return {
                "data": json_CoreFollowers,
                "commentary": commentary
            }

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

    # Return empty structure in case of error or no data
    return {
        "data": None,
        "commentary": ["No data available or an error occurred."]
    }


#====================================================================================================================================================================
# hashtag analysis
#====================================================================================================================================================================

def hashtag_analysis(hashtag_fb_df, hashtag_insta_df):
    def process_dataframe(df, platform):
        # Rename columns for Instagram platform
        if platform == 'insta':
            column_mapping = {
                'caption': 'message',
                'like_count': 'likes_count',
                'impression_count': 'impressions_count'
            }
            df = df.rename(columns=column_mapping)

        # Check for required columns
        required_columns = ['message', 'likes_count', 'comments_count', 'reach_count', 'impressions_count']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Missing columns in {platform} DataFrame: {missing_columns}")
            # Return an empty DataFrame if required columns are missing
            return pd.DataFrame(columns=['hashtag', 'repetition_count', 'likes_count', 'comments_count', 'reach_count', 'impressions_count'])

        # Initialize an empty list to store hashtags
        hashtag_data = []

        # Iterate over each row in the `message` column
        for message, likes, comments, reach, impressions in zip(
            df['message'], 
            df['likes_count'], 
            df['comments_count'], 
            df['reach_count'], 
            df['impressions_count']
        ):
            if pd.notna(message):  # Check if the message is not NaN
                # Find all words starting with '#' using regular expressions
                extracted_hashtags = re.findall(r'#\w+', message)

                # For each extracted hashtag, store the relevant data
                for hashtag in extracted_hashtags:
                    hashtag_data.append({
                        'hashtag': hashtag,
                        'likes_count': likes,
                        'comments_count': comments,
                        'reach_count': reach,
                        'impressions_count': impressions
                    })

        # Convert the list of hashtag data into a DataFrame
        expanded_df = pd.DataFrame(hashtag_data)
        
        if expanded_df.empty:
            return pd.DataFrame(columns=['hashtag', 'repetition_count', 'likes_count', 'comments_count', 'reach_count', 'impressions_count'])

        # Replace 'NA' strings with actual NaN
        numeric_columns = ['likes_count', 'comments_count', 'reach_count', 'impressions_count']
        expanded_df[numeric_columns] = expanded_df[numeric_columns].replace('NA', np.nan)

        # Convert columns to numeric (in case they are still strings)
        expanded_df[numeric_columns] = expanded_df[numeric_columns].apply(pd.to_numeric, errors='coerce')

        # Group by the hashtag and sum the counts
        hashtag_counts = expanded_df.groupby('hashtag')[numeric_columns].sum(min_count=1).reset_index()

        # Add repetition count
        hashtag_counts['repetition_count'] = expanded_df.groupby('hashtag').size().values

        # Reorder columns
        column_order = ['hashtag', 'repetition_count'] + numeric_columns
        hashtag_counts = hashtag_counts[column_order]

        # Replace all-NaN rows in numeric columns with 'NA'
        for col in numeric_columns:
            hashtag_counts[col] = hashtag_counts[col].where(hashtag_counts[col].notna(), 'NA')

        return hashtag_counts

    # Process both DataFrames
    hashtag_counts_fb = process_dataframe(hashtag_fb_df, 'fb')
    hashtag_counts_insta = process_dataframe(hashtag_insta_df, 'insta')

    # Function to create commentary
    def create_commentary(hashtag_counts, platform):
        if hashtag_counts.empty:
            return [f"No data available for {platform} platform."]

        def get_highest_metric(metric):
            # Make sure we're working with numeric values for comparison
            max_value = pd.to_numeric(hashtag_counts[metric], errors='coerce').max()
            
            # Check if max_value is valid
            if pd.isna(max_value) or max_value == 0:
                return [], 0
                
            # Find rows with the maximum value for this metric
            # Use pd.to_numeric again to ensure proper comparison
            numeric_column = pd.to_numeric(hashtag_counts[metric], errors='coerce')
            highest_rows = hashtag_counts[numeric_column == max_value]
            
            return highest_rows['hashtag'].tolist(), max_value

        commentary = [f"The Hashtag Table gives insights regarding the reach, impressions, likes, and comments of a particular hashtag."]

        for metric, label in [('reach_count', 'reach'), 
                             ('impressions_count', 'impressions'),
                             ('likes_count', 'likes count'), 
                             ('comments_count', 'comments count')]:
            hashtags, value = get_highest_metric(metric)
            
            if hashtags:
                # Format the value as a string with commas
                if pd.notna(value):
                    # Convert to int to remove any decimal points
                    formatted_value = f"{int(value):,}"
                else:
                    formatted_value = "N/A"
                
                commentary.append(
                    f"The hashtag(s) with the highest {label} is/are: {', '.join(hashtags)} with count {formatted_value}"
                )
            else:
                commentary.append(f"No hashtag has any {label}.")

        return commentary

    # Optional: Format counts for display
    def format_counts(data_list):
        for row in data_list:
            for col in ['reach_count', 'impressions_count', 'likes_count', 'comments_count']:
                if col in row and pd.notna(row[col]):
                    try:
                        # Make sure it's a number before formatting
                        row[col] = "{:,}".format(int(float(row[col])))
                    except (ValueError, TypeError):
                        # If it's already a string or can't be converted, leave it as is
                        pass
        return data_list

    # Generate commentary for both platforms
    fb_commentary = create_commentary(hashtag_counts_fb, 'Facebook')
    insta_commentary = create_commentary(hashtag_counts_insta, 'Instagram')

    # Convert DataFrames to JSON (with formatting)
    hashtag_counts_fb_json = format_counts(hashtag_counts_fb.to_dict(orient='records'))
    hashtag_counts_insta_json = format_counts(hashtag_counts_insta.to_dict(orient='records'))

    # Combine into a dictionary
    result = {
        'fb': {
            'data': hashtag_counts_fb_json,
            'commentary': fb_commentary
        },
        'insta': {
            'data': hashtag_counts_insta_json,
            'commentary': insta_commentary
        }
    }

    return result

#========================================================================================================================================================================
# Create a single MySQL connection to the server
def create_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host="45.76.160.28",
            user="Summer",
            password="Dragon",
        )
        return connection
    except mysql.connector.Error as e:
        print(f"Connection error: {e}")
        return None

# Get access token from correct database table
def get_access_token(platform, channel_unique_id, connection):
    db_name = 'social_media_fb' if platform == 'facebook' else 'social_media_insta'
    table_name = 'channels'

    try:
        connection.database = db_name
        cursor = connection.cursor(dictionary=True)
        query = f"SELECT token FROM {table_name} WHERE channel_unique_id = %s"
        cursor.execute(query, (channel_unique_id,))
        result = cursor.fetchone()
        if result:
            access_token = result['token']
            # print(f"[------------------------ Access Token for {platform} (ID: {channel_unique_id}): {access_token}")  # <-- Print here
            return access_token
        else:
            print(f"No access token found for {platform} (ID: {channel_unique_id})")
            return None
    except mysql.connector.Error as e:
        print(f"Query error in {db_name}: {e}")
    finally:
        cursor.close()
    return None

# Fetch followers/subscribers count
def fetch_followers_count(platform, channel_id, connection):
    try:
        if platform in ['facebook', 'instagram']:
            access_token = get_access_token(platform, channel_id, connection)
            if not access_token:
                print(f"No access token found for {platform} channel {channel_id}")
                return 0

            version = 'v12.0' if platform == 'facebook' else 'v7.0'
            url = f"https://graph.facebook.com/{version}/{channel_id}?fields=followers_count&access_token={access_token}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return int(data.get("followers_count", 0))

        elif platform == 'youtube':
            connection.database = "youtube"
            cursor = connection.cursor(dictionary=True)
            query = "SELECT Subscribers FROM youtube_subscribers_metrics WHERE ChannelId = %s ORDER BY CreatedOn DESC LIMIT 1"
            cursor.execute(query, (channel_id,))
            row = cursor.fetchone()
            cursor.close()
            return int(row['Subscribers']) if row else 0

    except requests.exceptions.RequestException as e:
        print(f"API error for {platform} {channel_id}: {e}")
    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None


# Get social media data
def get_social_media_data(platform, connection):
    channels = {
        "facebook": {"Health": "102447701156006"},
        "instagram": {"Health": "17841421377796071"},
        "youtube": {"Health": "16"}
    }

    if platform not in channels:
        print(f"Unsupported platform: {platform}")
        return {}

    platform_channels = channels[platform]
    followers_data = {}

    for channel_name, channel_id in platform_channels.items():
        current_count = fetch_followers_count(platform, channel_id, connection)
        followers_data[channel_name] = current_count

    return followers_data


# Aggregate all data
def current_followers_data(preApha=None, postApha=None, Apha=None):
    connection = create_mysql_connection()
    if not connection:
        return {
            "facebook_followers": 0,"instagram_followers": 0,"youtube_followers": 0}  # <-- return default dict instead of None

    try:
        facebook_followers = get_social_media_data('facebook', connection).get('Health', 0)
        instagram_followers = get_social_media_data('instagram', connection).get('Health', 0)
        youtube_followers = get_social_media_data('youtube', connection).get('Health', 0)
        return {
            "facebook_followers": facebook_followers,
            "instagram_followers": instagram_followers,
            "youtube_followers": youtube_followers
        }
    except Exception as e:
        print(f"Unexpected error in current_followers_data: {e}")
        return {"facebook_followers": 0,"instagram_followers": 0,"youtube_followers": 0}
    finally:
        connection.close()


#========================================================================================================================================================================
# CUMMULATIVE FOLLOWERS CHANGE 2024 VS 2025
#========================================================================================================================================================================

def apha_followers_cumulative_analysis(preApha=False, postApha=False, Apha=None):
    platform_details = get_all_platform_details()
    data = []

    # Get date(s) from helper
    date_or_range = get_date_or_range(preApha, postApha, Apha)

    if not isinstance(date_or_range, tuple):
        print("Invalid input to get_date_or_range")
        return

    date_2025, date_2024 = date_or_range

    # Case 1: Pre/post APHA with "to" range
    if "to" in date_2025 and "to" in date_2024:
        start_2025, end_2025 = date_2025.split(" to ")
        start_2024, end_2024 = date_2024.split(" to ")

        cumulative = False  # Not cumulative in this case

    # Case 2: APHA-specific
    else:
        cumulative = False
        if Apha == 1:
            current_2025 = pd.to_datetime(date_2025)
            current_2024 = pd.to_datetime(date_2024)

            start_2025 = (current_2025 - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            end_2025 = current_2025.strftime('%Y-%m-%d')

            start_2024 = (current_2024 - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            end_2024 = current_2024.strftime('%Y-%m-%d')

        elif Apha > 1:
            cumulative = True  # Enable cumulative mode

            def generate_day_ranges(n):
                date_ranges = []
                for day in range(1, n + 1):
                    d2025, d2024 = get_date_or_range(False, False, day)
                    date_2025 = pd.to_datetime(d2025)
                    date_2024 = pd.to_datetime(d2024)
                    prev_2025 = (date_2025 - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    prev_2024 = (date_2024 - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    date_ranges.append(((prev_2025, d2025), (prev_2024, d2024)))
                return date_ranges

            day_ranges = generate_day_ranges(Apha)

        else:
            print("Invalid APHA day specified.")
            return

    def get_follower_difference(db_details, date_start, date_end, db_year):
        try:
            connection = mysql.connector.connect(
                host="45.76.160.28",
                user="Summer",
                password="Dragon",
                database=db_details["db"]
            )
            cursor = connection.cursor()

            cursor.execute(f"""
                SELECT {db_details["column"]}
                FROM {db_details["table"]}
                WHERE {db_details["condition"]}
                AND DATE({db_details["date_column"]}) = %s
                ORDER BY {db_details["date_column"]} DESC
                LIMIT 1;
            """, (date_start,))
            start_result = cursor.fetchone()

            cursor.execute(f"""
                SELECT {db_details["column"]}
                FROM {db_details["table"]}
                WHERE {db_details["condition"]}
                AND DATE({db_details["date_column"]}) = %s
                ORDER BY {db_details["date_column"]} DESC
                LIMIT 1;
            """, (date_end,))
            end_result = cursor.fetchone()

            cursor.close()
            connection.close()

            if start_result and end_result:
                return start_result[0], end_result[0], end_result[0] - start_result[0]
            else:
                return 'None', 'None', 'None'

        except Exception as e:
            print(f"Error fetching data for {db_details['db']} ({db_year}): {e}")
            return 'None', 'None', 'None'

    for platform, db_details in platform_details.items():
        if cumulative:
            gain_2025_total = 0
            gain_2024_total = 0
            start_2025_val = end_2025_val = None
            start_2024_val = end_2024_val = None

            for (d2025, e2025), (d2024, e2024) in day_ranges:
                s25, e25, g25 = get_follower_difference(db_details, d2025.strip(), e2025.strip(), 2025)
                s24, e24, g24 = get_follower_difference(db_details, d2024.strip(), e2024.strip(), 2024)

                if g25 is not None:
                    gain_2025_total += g25
                    if start_2025_val is None:
                        start_2025_val = s25
                    end_2025_val = e25

                if g24 is not None:
                    gain_2024_total += g24
                    if start_2024_val is None:
                        start_2024_val = s24
                    end_2024_val = e24

            data.append({
                "Platform": platform,
                "2025 Start Date": day_ranges[0][0][0],
                "2025 End Date": day_ranges[-1][0][1],
                "2025 Start Value": start_2025_val,
                "2025 End Value": end_2025_val,
                "2025 Gain/Loss": gain_2025_total,
                "2024 Start Date": day_ranges[0][1][0],
                "2024 End Date": day_ranges[-1][1][1],
                "2024 Start Value": start_2024_val,
                "2024 End Value": end_2024_val,
                "2024 Gain/Loss": gain_2024_total,
            })

        else:
            start_2025_val, end_2025_val, gain_2025 = get_follower_difference(db_details, start_2025.strip(), end_2025.strip(), 2025)
            start_2024_val, end_2024_val, gain_2024 = get_follower_difference(db_details, start_2024.strip(), end_2024.strip(), 2024)

            data.append({
                "Platform": platform,
                "2025 Start Date": start_2025,
                "2025 End Date": end_2025,
                "2025 Start Value": start_2025_val,
                "2025 End Value": end_2025_val,
                "2025 Gain/Loss": gain_2025,
                "2024 Start Date": start_2024,
                "2024 End Date": end_2024,
                "2024 Start Value": start_2024_val,
                "2024 End Value": end_2024_val,
                "2024 Gain/Loss": gain_2024,
            })

    df = pd.DataFrame(data)
    # print("DATAFRAME : ", df)
    
    # Add dynamic commentary
    cumulative_followers_comparison_results = []

    for _, row in df.iterrows():
        commentary = []
        platform = row['Platform']
        gain_2025 = row['2025 Gain/Loss']
        gain_2024 = row['2024 Gain/Loss']

        # Build context for commentary
        context = (
            "pre-APHA" if preApha else
            "post-APHA" if postApha else
            f"APHA Day {Apha}" if Apha else
            "the APHA period"
        )

        # Create commentary for this platform
        if gain_2025 is not None and gain_2024 is not None:
            difference = gain_2025 - abs(gain_2024)
            
            if gain_2025 > gain_2024:
                commentary.append(f"On {platform}, during {context} the cumulative change in 2025 : {gain_2025}")
                commentary.append(f"And the cumulative change in 2024 : {gain_2024}")
                commentary.append(f"We Outperformed in 2025 by {int(difference):,}")
            elif gain_2025 < gain_2024:
                commentary.append(f"On {platform}, during {context} the cumulative change in 2025 : {gain_2025}")
                commentary.append(f"And the cumulative change in 2024 : {gain_2024}")
                commentary.append(f"We Underperformed in 2025 by {int(difference):,}.")
            else:
                commentary.append(f"On {platform}, the performance in 2025 matched that of 2024.")
                commentary.append("We gained same number of followers on both the years.")
        else:
            commentary.append(f"Data not available for {platform} to compare {context}.")
            

        # Filter DF to this platform only for plotting
        platform_df = df[df['Platform'] == platform]

        cumulative_followers_comparison_results.append({
            'platform': platform,
            'graphs_json': plot_cumulative_followers_comparison_graph_(platform_df),
            'commentary': commentary
        })

    return cumulative_followers_comparison_results



def plot_cumulative_followers_comparison_graph_(df):
    unique_platforms = df['Platform'].unique()

    for platform in unique_platforms:
        platform_df = df[df['Platform'] == platform]

        x_labels = ["2025                  2024"]
        gain_loss_2025 = platform_df['2025 Gain/Loss'].tolist()
        gain_loss_2024 = platform_df['2024 Gain/Loss'].tolist()

        # Graph title
        value_label = "Subscribers" if platform.lower() == 'youtube' else "Followers"
        title_text = f"APHA 2025 vs 2024 {platform} Cumulative Followers Change"


        # Calculate the maximum value (with extra space added)
        max_gain_loss = max(max(gain_loss_2025, default=0), max(gain_loss_2024, default=0))
        y_axis_max = max_gain_loss * 1.1  # Add 10% extra space above the highest value

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=x_labels,
            y=gain_loss_2025,
            name='2025',
            marker=dict(color='#c2a94f'),
            text=[f"{v:,.0f}" for v in gain_loss_2025],
            textposition='inside',
            textfont=dict(size=12, color='black')
        ))

        fig.add_trace(go.Bar(
            x=x_labels,
            y=gain_loss_2024,
            name='2024',
            marker=dict(color='#850000'),
            text=[f"{v:,.0f}" for v in gain_loss_2024],
            textposition='inside',
            textfont=dict(size=12, color='white')
        ))

        # Calculate ymax and add extra space
        ymax = max(max(gain_loss_2025, default=0), max(gain_loss_2024, default=0))
        y_axis_max = ymax * 1.1  # Adding 10% space above the ymax

        # Check if we have negative values to decide if we need to show negative y-axis
        if any(val < 0 for val in gain_loss_2025 + gain_loss_2024):
            y_axis_min = min(min(gain_loss_2025, default=0), min(gain_loss_2024, default=0)) * 1.1  # 10% extra below the lowest negative value
        else:
            y_axis_min = 0  # If no negative values, start y-axis at 0

        fig.update_layout(
            title={
                'text': title_text,
                'x': 0.5,
                'font': {'size': 12, 'color': '#c2a94f'}
            },
            xaxis=dict(
                title="Year",
                title_font=dict(family='Roboto', color='#c2a94f', size=12),
                tickfont=dict(family='Roboto', color='#c2a94f', size=12),
                showgrid=True,
                gridcolor='rgba(255,255,255,0.2)',
            ),
           yaxis=dict(
                title=f"{value_label} Gain/Loss",
                title_font=dict(family='Roboto', color='#c2a94f', size=12),
                tickfont=dict(family='Roboto', color='#c2a94f', size=12),
                showgrid=True,
                gridcolor='rgba(255,255,255,0.2)',
                tickformat=',.0f',
                range=[y_axis_min, y_axis_max]  # Apply dynamic y-axis range

            ),
            barmode='group',
            plot_bgcolor='black',
            paper_bgcolor='black',
            font=dict(color='#c2a94f'),
            legend=dict(
                title='Legend', 
                font=dict(size=10),
                orientation='h',
                x=0.5,  # Center the legend horizontally at the top
                xanchor='center',
                y=1,  # Position the legend at the top of the chart
                yanchor='bottom'  # Align the bottom of the legend with the top of the chart
            ),
            margin=dict(l=40, r=20, t=70, b=70)
        )

        return fig.to_json()




##################################################################################################################################################################
# DOD FOLLOWERS COMPARSION APHA 2024 VS APHA 2025
#########################################################################################################################################################################

def dod_followers_change_count(preApha=None, postApha=None, Apha=None):
    platform_details = get_all_platform_details()
    date_or_range = get_date_or_range(preApha, postApha, Apha)

    # Handle the case when date_or_range is not a tuple
    if not isinstance(date_or_range, tuple) or len(date_or_range) != 2:
        print(f"Invalid input to get_date_or_range: {date_or_range}")
        return
    
    # Get the date ranges for 2025 and 2024
    d2025, d2024 = date_or_range
    
    # Generate day-by-day date pairs for both years
    day_ranges = []
    
    if preApha or postApha:
        # For pre-APHA or post-APHA, we need to extract all days in the range
        if 'to' in str(d2025) and 'to' in str(d2024):
            # Parse the date ranges
            start_date_2025, end_date_2025 = d2025.split(" to ")
            start_date_2024, end_date_2024 = d2024.split(" to ")
            
            # Convert to datetime objects
            start_date_2025 = pd.to_datetime(start_date_2025.strip())
            end_date_2025 = pd.to_datetime(end_date_2025.strip())
            start_date_2024 = pd.to_datetime(start_date_2024.strip())
            end_date_2024 = pd.to_datetime(end_date_2024.strip())
            
            # Generate all days in the range
            date_range_2025 = pd.date_range(start_date_2025, end_date_2025)
            date_range_2024 = pd.date_range(start_date_2024, end_date_2024)
            
            # Make sure both ranges have the same number of days
            min_days = min(len(date_range_2025), len(date_range_2024))
            
            # Generate day ranges for each day in the range
            for i in range(min_days):
                current_date_2025 = date_range_2025[i].strftime('%Y-%m-%d')
                prev_date_2025 = (date_range_2025[i] - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                
                current_date_2024 = date_range_2024[i].strftime('%Y-%m-%d')
                prev_date_2024 = (date_range_2024[i] - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                
                day_ranges.append(((prev_date_2025, current_date_2025), (prev_date_2024, current_date_2024)))
        else:
            print("Invalid date range format")
            return
    elif Apha and Apha > 1:
        # For specific APHA days, generate day by day
        def generate_day_ranges(n):
            date_ranges = []
            for day in range(1, n + 1):
                day_date_pair = get_date_or_range(False, False, day)
                if not isinstance(day_date_pair, tuple) or len(day_date_pair) != 2:
                    print(f"Invalid date pair for day {day}: {day_date_pair}")
                    continue
                    
                d2025, d2024 = day_date_pair
                
                # Convert to datetime objects
                try:
                    date_2025 = pd.to_datetime(d2025)
                    date_2024 = pd.to_datetime(d2024)
                except Exception as e:
                    print(f"Date parsing error for day {day}: {e}")
                    print(f"Dates received: {d2025}, {d2024}")
                    continue
                    
                prev_2025 = (date_2025 - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                prev_2024 = (date_2024 - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                date_ranges.append(((prev_2025, d2025), (prev_2024, d2024)))
            return date_ranges

        day_ranges = generate_day_ranges(Apha)
    else:
        # Single APHA day
        for (start_2025, end_2025), (start_2024, end_2024) in zip([d2025], [d2024]):
            try:
                # Convert to datetime objects if not already
                date_2025 = pd.to_datetime(d2025)
                date_2024 = pd.to_datetime(d2024)
                
                prev_2025 = (date_2025 - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                prev_2024 = (date_2024 - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                
                # Add the range pair to day_ranges
                day_ranges.append(((prev_2025, d2025), (prev_2024, d2024)))
            except Exception as e:
                print(f"Date parsing error: {e}")
                print(f"Dates received: {d2025}, {d2024}")
                return
    
    print(f"Generated {len(day_ranges)} day pairs for analysis")

    all_data = []

    def fetch_value(details, date, year):
        try:
            connection = mysql.connector.connect(
                host="45.76.160.28",
                user="Summer",
                password="Dragon",
                database=details["db"]
            )
            cursor = connection.cursor(dictionary=True)

            # Main query
            if details["platform"].lower() == "youtube":
                query = f"""
                    SELECT {details['column']}
                    FROM {details['table']}
                    WHERE {details['condition']}
                    AND DATE({details['date_column']}) = '{date}'
                    ORDER BY {details['date_column']} DESC
                    LIMIT 1;
                """
            else:
                query = f"""
                    SELECT {details['column']}
                    FROM {details['table']}
                    WHERE {details['condition']}
                    AND DATE({details['date_column']}) = DATE_ADD('{date}', INTERVAL 1 DAY)
                    AND TIME(created_on) BETWEEN '11:00:00' AND '11:30:00';
                """

            cursor.execute(query)
            result = cursor.fetchone()

            # Fallback
            if not result and details["platform"].lower() != "youtube":
                fallback_query = f"""
                    SELECT {details['column']}
                    FROM {details['table']}
                    WHERE {details['condition']}
                    AND DATE({details['date_column']}) = '{date}'
                    ORDER BY {details['date_column']} DESC
                    LIMIT 1;
                """
                cursor.execute(fallback_query)
                result = cursor.fetchone()

            return result[details["column"]] if result else None

        except Exception as e:
            print(f"Error fetching data for {details['db']} ({year}): {e}")
            return 'None'
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    for platform, db_details in platform_details.items():
        db_details['platform'] = platform  # include platform in db_details for condition logic
        
        for i, ((start_2025, end_2025), (start_2024, end_2024)) in enumerate(day_ranges):
            # Fetch start and end values for both years
            val_2025_start = fetch_value(db_details, start_2025.strip(), 2025)
            val_2025_end = fetch_value(db_details, end_2025.strip(), 2025)
            
            val_2024_start = fetch_value(db_details, start_2024.strip(), 2024)
            val_2024_end = fetch_value(db_details, end_2024.strip(), 2024)
            
            # Calculate the gains/losses
            gain_25 = val_2025_end - val_2025_start if val_2025_end is not None and val_2025_start is not None else None
            gain_24 = val_2024_end - val_2024_start if val_2024_end is not None and val_2024_start is not None else None
            
            # Print debug info
            # print(f"Platform: {platform}, Day {i+1}")
            # print(f"2025: {start_2025} to {end_2025} - Start: {val_2025_start}, End: {val_2025_end}, Gain: {gain_25}")
            # print(f"2024: {start_2024} to {end_2024} - Start: {val_2024_start}, End: {val_2024_end}, Gain: {gain_24}")
            
            # Add day label based on the type of analysis
            if preApha:
                day_label = f"Pre-APHA Day {i+1}"
            elif postApha:
                day_label = f"Post-APHA Day {i+1}"
            else:
                day_label = f"Day {i+1}"
                
            # Add to dataset with proper labeling
            all_data.append({
                "Platform": platform, 
                "Date": end_2025, 
                "Year": 2025, 
                "Gain/Loss": gain_25,
                "Day": day_label
            })
            all_data.append({
                "Platform": platform, 
                "Date": end_2024, 
                "Year": 2024, 
                "Gain/Loss": gain_24,
                "Day": day_label
            })

    df = pd.DataFrame(all_data)

    cumulative_followers_comparison_results = []
    for platform in df['Platform'].unique():
        platform_df = df[df['Platform'] == platform]

        gain_2025_total = platform_df[platform_df['Year'] == 2025]['Gain/Loss'].sum()
        gain_2024_total = platform_df[platform_df['Year'] == 2024]['Gain/Loss'].sum()

        # Determine the context for the commentary
        if preApha:
            context = "Pre-APHA period"
        elif postApha:
            context = "Post-APHA period"
        elif Apha:
            context = f"APHA Day {Apha}" if Apha == 1 else f"first {Apha} days of APHA"
        else:
            context = "the analysis period"
            
        # Generate commentary
        if gain_2025_total > gain_2024_total:
            commentary = [f"On {platform}, we outperformed our {context} in 2025 compared to 2024."]
        elif gain_2025_total < gain_2024_total:
            commentary = [f"On {platform}, we underperformed our {context} in 2025 compared to 2024."]
        else:
            commentary = [f"On {platform}, the performance in 2025 matched that of 2024."]
            
        graph_json = plot_apha_dod_followers_comparison_graph_(platform_df, preApha=preApha, postApha=postApha, Apha=Apha)

        cumulative_followers_comparison_results.append({
            'platform': platform,
            'graphs_json': graph_json,
            'commentary': commentary
        })

    return cumulative_followers_comparison_results


def plot_apha_dod_followers_comparison_graph_(df, preApha=None, postApha=None, Apha=None):
    df = pd.DataFrame(df)
    unique_platforms = df['Platform'].unique()

    # Sort data by date to ensure chronological order
    df = df.sort_values(by=['Platform', 'Year', 'Date'])
    
    # Use Day column if it exists, otherwise create one
    if 'Day' not in df.columns:
        # Create a day label based on input parameters
        if preApha:
            period_type = "Pre-APHA "
        elif postApha:
            period_type = "Post-APHA "
        else:
            period_type = "Day "
            
        df['Day'] = df.groupby(['Platform', 'Year']).cumcount() + 1
        df['Day'] = period_type + df['Day'].astype(str)

    for platform in unique_platforms:
        platform_df = df[df['Platform'] == platform]

        # Create the figure
        fig = go.Figure()

        for year, color in zip([2025, 2024], ['#c2a94f', '#850000']):
            year_df = platform_df[platform_df['Year'] == year]

            fig.add_trace(go.Bar(
                x=year_df['Day'],
                y=year_df['Gain/Loss'],
                name=str(year),
                marker=dict(color=color),
                text=[f"{v:,.0f}" for v in year_df['Gain/Loss']],
                textposition='auto',
                textfont=dict(size=10)
            ))

        # Determine title based on input parameters
        if preApha:
            title_text = f"{platform} Gross Followers Count Change for Pre-APHA 2025 vs 2024"
            x_axis_title = "Pre-APHA Day"
        elif postApha:
            title_text = f"{platform} Gross Followers Count Change for Post-APHA 2025 vs 2024"
            x_axis_title = "Post-APHA Day"
        else:
            title_text = f"{platform} Gross Followers Count Change for APHA 2025 vs 2024"
            x_axis_title = "APHA Day"
            
        value_label = "Subscribers" if platform.lower() == 'youtube' else "Followers"

        fig.update_layout(
            title={
                'text': title_text,
                'x': 0.5,
                'font': {'size': 12, 'color': '#c2a94f'}
            },
            xaxis=dict(
                title=x_axis_title,
                title_font=dict(family='Roboto', color='#c2a94f', size=12),
                tickfont=dict(family='Roboto', color='#c2a94f', size=12),
                showgrid=True,
                gridcolor='rgba(255,255,255,0.2)',
                type='category'
            ),
            yaxis=dict(
                title=f"{value_label} Gain/Loss",
                title_font=dict(family='Roboto', color='#c2a94f', size=12),
                tickfont=dict(family='Roboto', color='#c2a94f', size=12),
                showgrid=True,
                gridcolor='rgba(255,255,255,0.2)',
                tickformat=',.0f'
            ),
            barmode='group',
            plot_bgcolor='black',
            paper_bgcolor='black',
            font=dict(color='#c2a94f'),
            legend=dict(
                title='Legend',
                font=dict(size=10),
                orientation='h',
                x=0.5,
                xanchor='center',
                y=1,
                yanchor='bottom'
            ),
            margin=dict(l=40, r=20, t=70, b=70)
        )

        # If we have many days, adjust the layout for better readability
        if len(platform_df['Day'].unique()) > 6:
            fig.update_layout(
                xaxis=dict(
                    tickangle=45,
                    tickfont=dict(family='Roboto', color='#c2a94f', size=10)
                ),
                margin=dict(l=40, r=20, t=70, b=100)  # Increase bottom margin for angled labels
            )

        # output_filename = f"{platform}_followers_{'preAPHA' if preAPHA else 'postAPHA' if postAPHA else 'APHA'}.jpeg"
        # fig.write_image(output_filename)
        # fig.show()

        return fig.to_json()







