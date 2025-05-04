from preswald import Workflow, text, plotly, connect, get_df, table, query, slider
import pandas as pd
import plotly.express as px

# Create a workflow instance
workflow = Workflow()

@workflow.atom()
def load_data():
    text("# Ocean Climate and Marine Life")
    # Connect to the data source and load the dataset
    connect()
    df = get_df("my_dataset_csv")

    return df

@workflow.atom()
def setup_slider():
    # Slider for filtering year
    current_year = slider(
        "Current Year",
        min_val=2015,
        max_val=2023,
        default=2015,
        step=1,
    )
    return current_year

@workflow.atom(dependencies=["load_data", "setup_slider"])
def create_scatter_for_year(load_data, setup_slider):
    df = load_data
    year = setup_slider

    # Convert Date, pH Level, and Species Observed columns to numeric
    df["Month"] = pd.to_numeric(df["Month"], errors="coerce")
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["pH Level"] = pd.to_numeric(df["pH Level"], errors="coerce")
    df["Species Observed"] = pd.to_numeric(df["Species Observed"], errors="coerce")

    # Filter the data based on the selected year
    filtered_df = df[df["Year"] == year]
    
    text(f"## pH Level vs. Species Observed\nThis scatter plot visualizes pH level and biodiversity in {year}.")

    if filtered_df is None or filtered_df.empty:
        text("No data available for this year.")
        return

    # Create the scatter plot
    try:
        fig = px.scatter(
            filtered_df,
            x="Month",
            y="Species Observed",
            color="pH Level",
            hover_name="Location",
            hover_data={
                "Month": False,
                "Species Observed": True,
                "pH Level": True,
            }
        )
        fig.update_layout(
            template="plotly_white",
            autosize=False,
            height=500,             # Fix size
            margin=dict(l=50, r=50, t=50, b=50)
        )

        # Add rectangle around plot
        fig.add_shape(
            type="rect",
            xref="paper", yref="paper",  # Use "paper" to anchor to full plot area (0 to 1)
            x0=0, y0=0, x1=1, y1=1,
            line=dict(color="black", width=1),
            fillcolor="rgba(0,0,0,0)",  # Transparent fill
            layer="above"
        )
        # Show the plot
        plotly(fig)
        
    except Exception as e:
        text(f"Error generating scatter plot: {str(e)}")

@workflow.atom(dependencies=["load_data", "setup_slider"])
def query_high_bleaching(load_data, setup_slider):
    current_year = setup_slider

    sql = f"""
        SELECT
            "Location",
            "Latitude",
            "Longitude",
            "pH Level",
            "SST (°C)",
            "Species Observed"
        FROM my_dataset_csv
        WHERE "Bleaching Severity" = 3
        AND "Year" = {current_year}
    """
    
    try:
        sql_df = query(sql, "my_dataset_csv")

        if sql_df is None or sql_df.empty:
            text("No data available for this year.")
            return

        text(f"## Locations with High Bleaching Severity in {current_year}")

        # Show the table
        table(sql_df)

        # Convert pH Level column to numeric
        sql_df["pH Level"] = pd.to_numeric(sql_df["pH Level"], errors="coerce")

        # Create a map of the locations with high bleaching
        fig_map = px.scatter_geo(
            sql_df,
            lat="Latitude",
            lon="Longitude",
            color="pH Level",
            hover_name="Location",
            hover_data={
                "Latitude": False,
                "Longitude": False,
                "pH Level": True,
                "Species Observed": True,
            }
        )

        fig_map.update_layout(
            height=500,             # Fix size
            margin=dict(l=0, r=0, t=30, b=0)
        )

        text(f"### Map of Locations with High Bleaching Severity in {current_year}")

        # Show the map
        plotly(fig_map)

    except Exception as e:
        text(f"Error querying high bleaching locations: {str(e)}")

@workflow.atom(dependencies=["load_data", "setup_slider"])
def query_averages(load_data, setup_slider):
    current_year = setup_slider

    sql1 = f"""
        SELECT
            "Location",
            ROUND(AVG(CAST("Latitude" AS FLOAT)), 4) AS avg_lat,
            ROUND(AVG(CAST("Longitude" AS FLOAT)), 4) AS avg_long,
            ROUND(AVG(CAST("Bleaching Severity" AS INTEGER)), 0) AS avg_bleach,
            ROUND(AVG(CAST("pH Level" AS FLOAT)), 2) AS avg_pH,
            ROUND(AVG(CAST("Species Observed" AS FLOAT)), 2) AS avg_species
        FROM my_dataset_csv
        WHERE "Year" = {current_year}
        GROUP BY "Location"
    """

    try:
        sql_df1 = query(sql1, "my_dataset_csv")

        if sql_df1 is None or sql_df1.empty:
            text("No data available for this year.")
            return
        
        # Rename columns
        sql_df1.rename(columns={
            "avg_lat": "Latitude",
            "avg_long": "Longitude",
            "avg_bleach": "Average Bleaching Severity",
            "avg_pH": "Average pH Level",
            "avg_species": "Average Species Observed"
        }, inplace=True)

        # Convert Lat, Lon, and Average Species Observed columns to numeric
        sql_df1["Latitude"] = pd.to_numeric(sql_df1["Latitude"], errors="coerce")
        sql_df1["Longitude"] = pd.to_numeric(sql_df1["Longitude"], errors="coerce")
        sql_df1["Average Species Observed"] = pd.to_numeric(sql_df1["Average Species Observed"], errors="coerce")

        # Calculate max and min bleaching severity for the given year
        max_severity = pd.to_numeric(sql_df1["Average Bleaching Severity"], errors="coerce").max()
        min_severity = pd.to_numeric(sql_df1["Average Bleaching Severity"], errors="coerce").min()

        # Map severity to numeric values
        severity_map = {
            0: "None",
            1: "Low",
            2: "Medium",
            3: "High"
        }
        sql_df1["Average Bleaching Severity"] = sql_df1["Average Bleaching Severity"].map(severity_map)


        overall_species = round(sum(sql_df1["Average Species Observed"]) / len(sql_df1["Average Species Observed"]), 2)
        print_max = severity_map.get(int(max_severity), "Unknown")
        print_min = severity_map.get(int(min_severity), "Unknown")
        if (print_max == print_min):
            text(
                f"## Map of Locations with Average pH Level and Average Species Observed in {current_year}"
                f"\nThe global average bleaching severity in {current_year} was {print_max}."
                f"\n\nThe average number of species observed was {overall_species}."
                )
        else:
            text(
                f"## Map of Locations with Average pH Level and Average Species Observed in {current_year}"
                 f"\nThe global average bleaching severity in {current_year} was {print_max} to {print_min}."
                 f"\n\nThe average number of species observed was {overall_species}."
                 )

        # Create a map of the locations with high bleaching
        fig_map1 = px.scatter_geo(
            sql_df1,
            lat="Latitude",
            lon="Longitude",
            color="Average Bleaching Severity",
            size="Average Species Observed",
            hover_name="Location",
            hover_data={
                "Latitude": False,
                "Longitude": False,
                "Average pH Level": True,
                "Average Species Observed": True,
                "Average Bleaching Severity": True
            },
            color_discrete_map={
                "None": "lightgreen",
                "Low": "cornflowerblue",
                "Medium": "plum",
                "High": "salmon"
            },
            opacity=0.8 
        )

        fig_map1.update_layout(
            height=500,             # Fix size
            margin=dict(l=0, r=0, t=30, b=0)
        )

        # Show the map
        plotly(fig_map1)

    except Exception as e:
        text(f"Error querying averages: {str(e)}")

# Execute the workflow
workflow.execute()

