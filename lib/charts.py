# Script to generate statistics based on the given changeset CSV via stdin.
# The script creates an index.html file.

import numpy as np
import pandas as pd
import altair as alt
import panel as pn
import geopandas as gpd
import pyproj
import sys
from shapely.geometry import box

mainPage = pn.Column(
    pn.pane.Markdown("""# OpenStop Statistics"""),
    stylesheets=[
        """
        .markdown {
            max-width: 700px;
            width: 100%;
            margin: 0 auto;
            font-size: 1rem;
        }

        .bk-panel-models-vega-VegaPlot,
        .bk-panel-models-tabulator-DataTabulator {
            margin: 0 auto;
        }

        .narrow-chart {
            max-width: 700px;
            width: 100%;
        }
        """
    ]
).servable()

# Load country boundaries
countries = gpd.read_file('https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip').to_crs(epsg=4326)
# Generate country emoji flags and add additional column
# Use _EH because https://github.com/nvkelso/natural-earth-vector/issues/268#issuecomment-778832542
countries['flag'] = countries['ISO_A2_EH'].map(
    lambda country_code : ''.join(chr(127397 + ord(char)) for char in country_code)
)
# Load changeset data
changesets_data = pd.read_csv(sys.stdin, parse_dates=['created_at', 'closed_at'])
# Drop changesets without a bbox
changesets_data.dropna(
    axis=0,
    subset=['min_lat','min_lon','max_lat','max_lon'],
    inplace=True
)
# Convert dataframe to GeoDataFrame
def create_bbox(row):
    return box(row['min_lon'],row['min_lat'],row['max_lon'],row['max_lat'])
changesets_data = gpd.GeoDataFrame(
    changesets_data,
    geometry=changesets_data.apply(create_bbox, axis=1),
    crs=4326
)
# Spatial join between changesets and countries
changesets_data = gpd.sjoin(
    changesets_data,
    countries,
    how='left',
    predicate='intersects'
)

mainPage.append(
    pn.pane.Markdown("""
        ## Changes and contributor activity
        Shows the number of changed elements and active users per day.
    """)
)
# Group by 'created_date' day and count the number of changesets and distinct users in each group
df = (changesets_data
  .groupby(changesets_data['created_at'].dt.date)
  .agg(
    changeset_count = ('num_changes', 'sum'),
    user_count = ('uid', 'nunique'),
  )
)
# Create a complete date range from min to max date
min_date = df.index.min()
max_date = df.index.max()
date_range = pd.date_range(start=min_date, end=max_date, freq='D').date
# Reindex the DataFrame to include all dates and fill missing values with 0
df = df.reindex(date_range, fill_value=0).reset_index()
# Create charts
resize = alt.selection_interval(encodings=['x'], bind='scales')
base = alt.Chart(df).encode(
    x=alt.X('created_at').title(None)
).properties(
    height=300,
    width=700, # auto width doesn't work see https://github.com/vega/altair/issues/1734#issuecomment-1413185087
).add_params(
    resize
)
chart1 = base.mark_line(
    stroke='#ff0000'
).encode(
    y=alt.Y('changeset_count').axis(title='Changed elements count'),
)
chart2 = base.mark_line(
    stroke='#5276A7'
).encode(
    y=alt.Y('user_count').axis(title='Contributor count'),
)
mainPage.append(pn.pane.Vega(alt.vconcat(chart1, chart2)))

#########################################

mainPage.append(
    pn.pane.Markdown("""
        ## Total element changes over time
        This shows the accumulated amount of changed elements over time.
    """)
)
# Group by 'created_at' day and sum the number of changes in each group
df = (changesets_data
  .groupby(changesets_data['created_at'].dt.date)
  .agg(
    changes = ('num_changes', 'sum'),
  )
  .reset_index()
)
# Accumulate changes count
df['accumulated_changes'] = df['changes'].cumsum()
# Create chart
chart = alt.Chart(df).mark_area(
    fill='#ff0000'
).encode(
    x=alt.X('created_at').title(None),
    y=alt.Y('accumulated_changes').axis(title='Total changed elements'),
).properties(
    height=300,
    width='container'
)
mainPage.append(pn.pane.Vega(chart, css_classes=['narrow-chart']))

#########################################

mainPage.append(
    pn.pane.Markdown("""
        ## Total contributors over time
        This shows the accumulated amount of contributors over time.
    """)
)
# Only keep one row per user, remove subsequent occurrences
df = changesets_data.drop_duplicates('uid', inplace=False)
# Group by 'created_at' day and count the number of entries in each group
df = (df
  .groupby(df['created_at'].dt.date)
  .agg(
    users = ('uid', 'size'),
  )
  .reset_index()
)
# Accumulate user count
df['accumulated_users'] = df['users'].cumsum()
# Create chart
chart = alt.Chart(df).mark_area().encode(
    x=alt.X('created_at').title(None),
    y=alt.Y('accumulated_users').axis(title='Total contributors'),
).properties(
    height=300,
    width='container'
)
mainPage.append(pn.pane.Vega(chart, css_classes=['narrow-chart']))

#########################################

mainPage.append(
    pn.pane.Markdown("""
        ## Seasonal user participation (per month)
        This shows the average changed elements and active users per month.<br>
        **Note:** Data before 2023 is excluded because the app was first released in the mids of 2022.
        Data from the current year is excluded to reduce distortion.
    """)
)

df = changesets_data.loc[
    # Filter dates before 2023 as the app was introduced in the mids of 2022
    (changesets_data['created_at'] >= '2023-01-01') &
    # Filter dates before the end of the current year
    (changesets_data['created_at'] < pd.Period.now('Y').strftime('%Y-01-01'))
]
# Group by month and year and count the number of changesets and distinct users in each group
df = (df
  .groupby([
    df['created_at'].dt.month,
    df['created_at'].dt.year
  ])
  .agg(
    total_changes = ('num_changes', 'sum'),
    total_users = ('uid', 'nunique'),
  )
  # Group again by month and calculate average activity
  .groupby(level=0)
  .agg(
    total_changes = ('total_changes', 'mean'),
    total_users = ('total_users', 'mean'),
  )
)
# Add additional month name column
df['month_name'] = pd.to_datetime(df.index, format='%m').month_name()
# Draw bar charts
base = alt.Chart(df).encode(
    y=alt.Y('created_at:O').axis(None).sort('descending'),
)
left = base.mark_bar(
    fill='#ff0000'
).encode(
    x=alt.X('total_changes:Q').title('Total changed elements').sort('descending'),
    tooltip=alt.Tooltip('total_changes', title='Total changed elements'),
)
middle = base.mark_text().encode(
    text=alt.Text('month_name:O'),
).properties(
    view=alt.ViewConfig(strokeWidth=0),
    title=alt.Title('Month')
)
right = base.mark_bar().encode(
    x=alt.X('total_users:Q').title('Total users'),
    tooltip=alt.Tooltip('total_users', title='Contributors'),
)
mainPage.append(
    pn.pane.Vega(alt.concat(left, middle, right, spacing=5))
)

#########################################

mainPage.append(
    pn.pane.Markdown("""
        ## Annual user participation (per year)
        This shows the total changed elements and active users per year.
    """)
)
# Group by 'created_date' and count the number of changesets and distinct users in each group
df = (changesets_data
  .groupby(changesets_data['created_at'].dt.year)
  .agg(
    total_changes = ('num_changes', 'sum'),
    total_users = ('uid', 'nunique'),
  )
)
# Draw bar charts
base = alt.Chart(df).encode(
    y=alt.Y('created_at:N').axis(None).sort('descending'),
)
left = base.mark_bar(
    fill='#ff0000'
).encode(
    x=alt.X('total_changes:Q').title('Total changed elements').sort('descending'),
    tooltip=alt.Tooltip('total_changes', title='Total changed elements'),
)
middle = base.mark_text().encode(
    text=alt.Text('created_at:N'),
).properties(
    view=alt.ViewConfig(strokeWidth=0),
    title=alt.Title('Year')
)
right = base.mark_bar().encode(
    x=alt.X('total_users:Q').title('Total users'),
    tooltip=alt.Tooltip('total_users', title='Contributors'),
)
mainPage.append(
    pn.pane.Vega(alt.concat(left, middle, right, spacing=5))
)

#########################################

mainPage.append(
    pn.pane.Markdown("""
        ## User engagement by contributions
        This groups contributors based on the number of changed elements (contributions).
        It shows the number of contributors per group and their collective contributions.
        """
    )
)
# Group by 'uid' and count the number of entries in each group
df = (changesets_data
  .groupby('uid', as_index=False)
  .agg(
    total_changes = ('num_changes', 'sum'),
  )
  .sort_values('total_changes', ascending=False)
)
# Bin data / add categories
df['category'] = pd.cut(
    df['total_changes'],
    bins=[1, 10, 100, 500, 1000, float("inf")],
    labels=['1-10', '10-100', '100-500', '500-1000', '> 1000'],
    include_lowest=True
)
# Group data by category
df = (df
    .groupby('category', observed=True)
    .agg(
        total_users = ('uid', 'size'),
        total_changes = ('total_changes', 'sum'),
    )
)
# Draw bar charts
base = alt.Chart(df).encode(
    y=alt.Y('category:O').axis(None).sort('descending'),
)
left = base.mark_bar(
    fill='#ff0000'
).encode(
    x=alt.X('total_changes:Q').title('Total changed elements').sort('descending'),
    tooltip=alt.Tooltip('total_changes', title='Total changed elements'),
)
middle = base.mark_text().encode(
    text=alt.Text('category:N'),
).properties(
    view=alt.ViewConfig(strokeWidth=0),
    title=alt.Title(
       ['Users grouped','by contributions',]
    )
)
right = base.mark_bar().encode(
    x=alt.X('total_users:Q').title('Total users'),
    tooltip=alt.Tooltip('total_users', title='Contributors'),
)
mainPage.append(
    pn.pane.Vega(alt.concat(left, middle, right, spacing=5))
)

#########################################

# Aggregate changes per country
df = (changesets_data
    .groupby('NAME', as_index=False)
    .agg(
        total_changes=('num_changes', 'sum'),
        total_users=('uid', 'nunique'),
    )
)
# Merge countries back to also get all unmatched countries
df = countries.merge(df, on='NAME', how='left')
# Fill empty fields with 0
df['total_changes'] = df['total_changes'].fillna(0)
df['total_users'] = df['total_users'].fillna(0)

mainPage.append(
    pn.pane.Markdown("""
        ## Changed elements per country
        This shows the total amount of changed elements per country.
    """)
)
# Create the choropleth map
choropleth = alt.Chart(df).mark_geoshape(
    stroke='black',
    strokeWidth=0.2
).encode(
    color=alt.Color(
        'total_changes:Q',
        scale=alt.Scale(
            type='log',
            # domain starts at 1 because log cannot be 0, this requires clamp=true
            # alternative would be to use symlog
            domain=[1, df['total_changes'].max()],
            range=['#fff', '#fff1f3', '#ff0000'],
            clamp=True
        ),
        legend=alt.Legend(title='Number of changed elements')
    ),
    tooltip=[
        alt.Tooltip('NAME:N', title='Country'),
        alt.Tooltip('total_changes:Q', title='Total changed elements')
    ]
).project(
    type='equalEarth'
).properties(
    width=1000,
    height=600,
    title='Changes Distribution by Country'
)
mainPage.append(pn.pane.Vega(choropleth))

mainPage.append(
    pn.pane.Markdown("""
        ## Contributors per country
        This shows the total amount of users that contributed to a particular country.<br>
        **Note:** This does not show where the users are from. If a user contributed to multiple countries they will be counted for each country.
    """)
)
# Create the choropleth map
choropleth = alt.Chart(df).mark_geoshape(
    stroke='black',
    strokeWidth=0.2
).encode(
    color=alt.Color(
        'total_users:Q',
        scale=alt.Scale(
            type='log',
            # domain starts at 1 because log cannot be 0, this requires clamp=true
            # alternative would be to use symlog
            domain=[1, df['total_users'].max()],
            range=['#fff', '#f1f7ff', '#5276A7'],
            clamp=True
        ),
        legend=alt.Legend(title='Number of Contributors')
    ),
    tooltip=[
        alt.Tooltip('NAME:N', title='Country'),
        alt.Tooltip('total_users:Q', title='Total Contributors')
    ]
).project(
    type='equalEarth'
).properties(
    width=1000,
    height=600,
    title='Contributors Distribution by Country'
)
mainPage.append(pn.pane.Vega(choropleth))

#########################################

mainPage.append(
    pn.pane.Markdown("""
        ## Country ranking
        All countries in which elements were changed via OpenStop.
    """)
)
country_ranking = (changesets_data
    .groupby(['NAME', 'flag'], as_index=False).agg(
        total_changes=('num_changes', 'sum'),
        total_users=('uid', 'nunique'),
    ).sort_values('total_changes', ascending=False)
)
# Filter columns
country_ranking = country_ranking[['flag', 'NAME', 'total_changes', 'total_users']]
# Show data table
mainPage.append(
    pn.widgets.Tabulator(
        country_ranking,
         height=300,
        disabled=True,
        show_index=False,
        titles={
            'flag': 'Flag',
            'NAME': 'Country',
            'total_changes': 'Total changed elements',
            'total_users': 'Total contributors',
        }
    )
)


mainPage.save('index.html', title="OpenStop Statistics")
