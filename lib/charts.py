# Script to generate statistics based on the given changeset CSV via stdin.
# The script creates an index.html file.

import numpy as np
import pandas as pd
import altair as alt
import geopandas as gpd
import pyproj
import sys
from pathlib import Path
from shapely.geometry import box

def changes_activity(changesets_data, release_marks):
    # Group by 'created_date' day and count the number of changesets and distinct users in each group
    df = (changesets_data
        .groupby(pd.Grouper(key='created_at', freq='D'))
        .agg(
            changeset_count = ('num_changes', 'sum'),
            user_count = ('uid', 'nunique'),
        )
    )
    # Create a complete date range from min to max date
    date_range = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq='D'
    )
    # Reindex the DataFrame to include all dates and fill missing values with 0
    df = df.reindex(
        date_range,
        fill_value=0
    ).rename_axis(['created_at']).reset_index()
    # Create charts
    resize = alt.selection_interval(encodings=['x'], bind='scales')
    base = alt.Chart(df).encode(
        x=alt.X('created_at').title(None)
    ).properties(
        height=300,
        width="container"
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
    return alt.vconcat(
        release_marks + chart1,
        release_marks + chart2
     # enable auto width see https://github.com/vega/altair/issues/1734#issuecomment-1413185087
    ).configure(
        autosize={
            "type": "fit-x",
            "contains": "padding"
        }
    )

#########################################

def total_changes(changesets_data, release_marks):
    # Group by 'created_at' day and sum the number of changes in each group
    df = (changesets_data
        .groupby(pd.Grouper(key='created_at', freq='D'))
        .agg(
            changes = ('num_changes', 'sum'),
        )
        .reset_index()
    )
    # Accumulate changes count
    df['accumulated_changes'] = df['changes'].cumsum()
    # Create chart
    return (release_marks +
        alt.Chart(df).mark_area(
            fill='#ff0000'
        ).encode(
            x=alt.X('created_at').title(None),
            y=alt.Y('accumulated_changes').axis(title='Total changed elements'),
            tooltip=[
                alt.Tooltip('created_at', title='Date'),
                alt.Tooltip('accumulated_changes', title='Changed elements'),
            ]
        )
    ).properties(
        height=300,
        width='container',
        autosize={
            "type": "fit-x",
            "contains": "padding"
        },
    )

#########################################

def total_contributors(changesets_data, release_marks):
    # Only keep one row per user, remove subsequent occurrences
    df = changesets_data.drop_duplicates('uid', inplace=False)
    # Group by 'created_at' day and count the number of entries in each group
    df = (df
        .groupby(pd.Grouper(key='created_at', freq='D'))
        .agg(
            users = ('uid', 'size'),
        )
        .reset_index()
    )
    # Accumulate user count
    df['accumulated_users'] = df['users'].cumsum()
    # Create chart
    return (release_marks +
        alt.Chart(df).mark_area().encode(
            x=alt.X('created_at').title(None),
            y=alt.Y('accumulated_users').axis(title='Total contributors'),
            tooltip=[
                alt.Tooltip('created_at', title='Date'),
                alt.Tooltip('accumulated_users', title='Contributor count'),
            ]
        )
    ).properties(
        height=300,
        width='container',
        autosize={
            "type": "fit-x",
            "contains": "padding"
        },
    )
#########################################

def seasonal_participation(changesets_data):
    df = changesets_data.loc[
        # Filter dates before 2023 as the app was introduced in the mids of 2022
        (changesets_data['created_at'] >= '2023-01-01') &
        # Filter dates before the end of the current year
        (changesets_data['created_at'] < pd.Period.now('M').strftime('%Y-%m-01'))
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
        ).reset_index()
    )
    # Add additional month name column
    df['month_name'] = pd.to_datetime(df['created_at'], format='%m').dt.month_name()
    # Draw bar charts
    base = alt.Chart(df).encode(
        y=alt.Y('created_at:O').axis(None).sort('descending'),
    )
    left = base.mark_bar(
        fill='#ff0000'
    ).encode(
        x=alt.X('total_changes:Q').title('Average changed elements').sort('descending'),
        tooltip=alt.Tooltip('total_changes', title='Average changed elements'),
    )
    middle = base.mark_text().encode(
        text=alt.Text('month_name:O'),
    ).properties(
        view=alt.ViewConfig(strokeWidth=0),
        title=alt.Title('Month')
    )
    right = base.mark_bar().encode(
        x=alt.X('total_users:Q').title('Average active users'),
        tooltip=alt.Tooltip('total_users', title='Contributors'),
    )
    return alt.concat(left, middle, right, spacing=5)

#########################################

def annual_participation(changesets_data):
    # Group by 'created_date' and count the number of changesets and distinct users in each group
    df = (changesets_data
        .groupby(pd.Grouper(key='created_at', freq='YE'))
        .agg(
            total_changes = ('num_changes', 'sum'),
            total_users = ('uid', 'nunique'),
        ).reset_index()
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
        text=alt.Text('year(created_at):N'),
    ).properties(
        view=alt.ViewConfig(strokeWidth=0),
        title=alt.Title('Year')
    )
    right = base.mark_bar().encode(
        x=alt.X('total_users:Q').title('Total active users'),
        tooltip=alt.Tooltip('total_users', title='Contributors'),
    )
    return alt.concat(left, middle, right, spacing=5)

#########################################

def user_engagement(changesets_data):
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
    ).reset_index()
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
        x=alt.X('total_users:Q').title('Total active users'),
        tooltip=alt.Tooltip('total_users', title='Contributors'),
    )
    return alt.concat(left, middle, right, spacing=5)

#########################################

def country_changes(changesets_data, countries):
    # Aggregate changes per country
    df = (changesets_data
        .groupby('NAME', as_index=False)
        .agg(total_changes=('num_changes', 'sum'))
    )
    # Merge countries back to also get all unmatched countries
    df = countries.merge(df, on='NAME', how='left')
    # Fill empty fields with 0
    df['total_changes'] = df['total_changes'].fillna(0)
    # Filter columns
    df = df[['NAME', 'total_changes', 'geometry']]
    # Create the choropleth map
    return alt.Chart(df).mark_geoshape(
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
        title='Changes Distribution by Country',
        height=600,
        width='container',
        autosize={
            "type": "fit-x",
            "contains": "padding"
        },
    )

#########################################

def country_contributors(changesets_data, countries):
    # Aggregate changes per country
    df = (changesets_data
        .groupby('NAME', as_index=False)
        .agg(total_users=('uid', 'nunique'))
    )
    # Merge countries back to also get all unmatched countries
    df = countries.merge(df, on='NAME', how='left')
    # Fill empty fields with 0
    df['total_users'] = df['total_users'].fillna(0)
    # Filter columns
    df = df[['NAME', 'total_users', 'geometry']]
    # Create the choropleth map
    return alt.Chart(df).mark_geoshape(
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
        title='Contributors Distribution by Country',
        height=600,
        width='container',
        autosize={
            "type": "fit-x",
            "contains": "padding"
        },
    )

#########################################

def country_ranking(changesets_data):
    country_ranking = (changesets_data
        .groupby(['NAME', 'flag'], as_index=False).agg(
            total_changes=('num_changes', 'sum'),
            total_users=('uid', 'nunique'),
        ).sort_values('total_changes', ascending=False)
    )
    # Filter columns
    return country_ranking[['flag', 'NAME', 'total_changes', 'total_users']]

#########################################

def countries_timeline(changesets_data):
    # Get top 10 countries by contributions
    top_10_countries = (changesets_data
        .groupby('NAME', as_index=False).agg(
            total_changes=('num_changes', 'sum'),
        ).sort_values(
            'total_changes',
            ascending=False
        ).iloc[:10]
    )
    # Aggregate changes per country
    df = (changesets_data
        .groupby([
            'NAME',
            pd.Grouper(key='created_at', freq='ME')
        ]).agg(
            total_changes=('num_changes', 'sum'),
            total_users=('uid', 'nunique'),
        ).reset_index()
    )
    # Filter rows by top 10 countries
    df = df.loc[df['NAME'].isin(top_10_countries['NAME'])]

    # Create stacked area chart
    return alt.Chart(df).transform_calculate(
        # Add artificial country rank column
        # See: https://stackoverflow.com/a/61343643
        rank=f'{str({v: i for i, v in enumerate(top_10_countries['NAME'])})}[datum.NAME]'
    ).mark_area(
        interpolate='basis'
    ).encode(
        x=alt.X(
            'created_at:T',
            title=None
        ),
        y=alt.Y(
            'total_changes:Q',
            title='Total changed elements'
        ),
        color=alt.Color(
            'NAME:N',
            sort=alt.SortField('rank', 'ascending'),
            title='Countries'
        ),
        tooltip=[
            alt.Tooltip('NAME:N', title='Country'),
            alt.Tooltip('total_changes:Q', title='Total changed elements'),
            alt.Tooltip('yearmonth(created_at):T', title='Date'),
        ],
        # Sort areas by added country rank column
        order=alt.Order(
            'rank:O',
            sort='descending'
        )
    ).properties(
        height=600,
        width='container',
        autosize={
            "type": "fit-x",
            "contains": "padding"
        },
    )

#########################################

def user_contributions(changesets_data):
    # Aggregate first and last user contribution
    df = (changesets_data
        .groupby('uid').agg(
            first_changeset=('created_at', 'min'),
            last_changeset=('created_at', 'max'),
            total_changes=('num_changes', 'sum'),
        )
    )
    domain = [1, df['total_changes'].max()]
    # Create scatter plot
    return alt.Chart(df).mark_circle().encode(
        x=alt.X(
            'first_changeset',
            title='First contribution'),
        y=alt.Y(
            'last_changeset',
            title='Last contribution'),
        size=alt.Size(
            'total_changes:Q',
        ).scale(domain=domain),
        color=alt.Color(
            'total_changes:Q',
            title='Total changed elements'
        ).scale(
            range=['red','green'],
            domain=domain
        ),
        tooltip=[
            alt.Tooltip('first_changeset:T', title='First contribution'),
            alt.Tooltip('last_changeset:T', title='Last contribution'),
            alt.Tooltip('total_changes:Q', title='Total changed elements')
        ],
    ).configure_scale(
        minSize=30
    ).properties(
        height=600,
        width='container',
        autosize={
            "type": "fit-x",
            "contains": "padding"
        },
    ).interactive()

#########################################

if __name__ == '__main__':
    # Load OpenStop GitHub release data
    openstop_releases = pd.read_json('https://api.github.com/repos/OPENER-next/OpenStop/releases', orient='records')
    # Filter by columns
    openstop_releases = openstop_releases[['published_at', 'name']]
    # Construct release marks chart
    release_marks = alt.Chart(openstop_releases).mark_rule(
        strokeDash=[8,8],
    ).encode(
        x=alt.X('published_at'),
        size=alt.value(1),
        tooltip=[
            alt.Tooltip('name', title='Release'),
            alt.Tooltip('published_at', title='Date'),
        ]
    )
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
    # Chart directory
    chart_dir = f'{sys.path[0]}/web/charts/'
    Path(chart_dir).mkdir(parents=True, exist_ok=True)
    # Create charts
    changes_activity(changesets_data, release_marks).save(
        f'{chart_dir}changes_activity.json'
    )
    total_changes(changesets_data, release_marks).save(
        f'{chart_dir}total_changes.json'
    )
    total_contributors(changesets_data, release_marks).save(
        f'{chart_dir}total_contributors.json'
    )
    seasonal_participation(changesets_data).save(
        f'{chart_dir}seasonal_participation.json'
    )
    annual_participation(changesets_data).save(
        f'{chart_dir}annual_participation.json'
    )
    user_engagement(changesets_data).save(
        f'{chart_dir}user_engagement.json'
    )
    country_changes(changesets_data, countries).save(
        f'{chart_dir}country_changes.json'
    )
    country_contributors(changesets_data, countries).save(
        f'{chart_dir}country_contributors.json'
    )
    country_ranking(changesets_data).to_json(
        f'{chart_dir}country_ranking.json',
        orient='records'
    )
    countries_timeline(changesets_data).save(
        f'{chart_dir}countries_timeline.json'
    )
    user_contributions(changesets_data).save(
        f'{chart_dir}user_contributions.json'
    )
