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
    df = get_df('my_dataset_csv')
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

    # Convert Year column to numeric
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')

    # Filter the data based on the selected year
    filtered_df = df[df['Year'] == year]
    
    text(f'## pH Level vs. Species Observed\nThis scatter plot visualizes pH level and biodiversity in {year}.')

    if filtered_df.empty:
        text("No data available for this year.")
        return

    # Create the scatter plot
    try:
        fig = px.scatter(
            filtered_df,
            x='Month',
            y='Species Observed',
            color='pH Level',
            hover_name='Location',
            hover_data={
                'Month': False,
                'Species Observed': True,
                'pH Level': True,
            },
        )
        fig.update_layout(template='plotly_white')

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
        WHERE "Bleaching Severity" = 'High'
        AND "Year" = {current_year}
    """
    
    try:
        sql_df = query(sql, 'my_dataset_csv')
        text(f'## Locations with High Bleaching Severity in {current_year}')

        # Show the table
        table(sql_df)

        # Create a map of the locations with high bleaching
        fig_map = px.scatter_geo(
            sql_df,
            lat='Latitude',
            lon='Longitude',
            color='pH Level',
            hover_name='Location',
            hover_data={
                'Latitude': False,
                'Longitude': False,
                'pH Level': True,
                'Species Observed': True,
            },
        )

        text(f'### Map of Locations with High Bleaching Severity in {current_year}')

        # Show the map
        plotly(fig_map)

    except Exception as e:
        text(f"Error querying high bleaching locations: {str(e)}")

@workflow.atom(dependencies=["load_data", "setup_slider"])
def query_averages(load_data, setup_slider):
    current_year = setup_slider
    text(f'## Map of Locations with Average pH Level and Average Species Observed in {current_year}')

    sql1 = f"""
        SELECT
            "Location",
            MEDIAN("Bleaching Severity") AS 'Median Bleaching Severity',
            AVG("Latitude") AS avg_lat,
            AVG("Longitude") AS avg_long,
            ROUND(AVG("pH Level"), 2) AS 'Average pH Level',
            ROUND(AVG("Species Observed"), 2) AS 'Average Species Observed'
        FROM my_dataset_csv
        WHERE "Year" = {current_year}
        GROUP BY "Location"
    """

    try:
        sql_df1 = query(sql1, 'my_dataset_csv')

        # Convert Average Species Observed column to numeric
        sql_df1['Average Species Observed'] = pd.to_numeric(sql_df1['Average Species Observed'], errors='coerce')

        # Create a map of the locations with high bleaching
        fig_map1 = px.scatter_geo(
            sql_df1,
            lat='avg_lat',
            lon='avg_long',
            color='Median Bleaching Severity',
            size='Average Species Observed',
            hover_name='Location',
            hover_data={
                'avg_lat':False,
                'avg_long':False,
                'Average pH Level':True,
                'Average Species Observed':True,
                'Median Bleaching Severity':True,
            },
        )

        # Show the map
        plotly(fig_map1)

    except Exception as e:
        text(f"Error querying averages: {str(e)}")

# Execute the workflow
workflow.execute()

