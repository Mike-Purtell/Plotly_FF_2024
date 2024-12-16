'''
This script produces:
    - choropleth map of Rural investment by US county
    - Pareto showing investment amounts grouped by state or territory
    - Pareto with investment amounts grouped by region.

hover info:
    colorpleth map: county name, state abbreviation, investment amount
    pareto by state: state name, investment amount, cumulative %
    pareto by region: region name, investment amount, cumulative %

pareto by state filtered to only include 50 US states
pareto by region merged smallest invested regions to group called others
    
'''
import polars as pl   # dataframe library
import us             # library with USA state info(name, abbr, timezone, etc.)
import plotly.express as px
from plotly.subplots import make_subplots

#------------------------------------------------------------------------------#
#     Functions                                                                #
#------------------------------------------------------------------------------#
def add_annotation(fig, annotation, x, y, align, xanchor, yanchor):
    fig.add_annotation(
        text=annotation,
        showarrow=False,
        xref = 'paper', x=x, yref = 'paper', y=y,
        align= align, xanchor=xanchor, yanchor=yanchor,
        font =  {'size': 14, 'color': 'darkslategray'}
    )
    return fig

def update_layout(
        fig, my_title, my_height, my_width,              # mandatory parameters
        my_xtitle='', my_ytitle='', my_legend_title='',  # optional parameters
        my_showlegend=False
    ):
    fig.update_layout(
        title = my_title,
        xaxis_title=my_xtitle,
        yaxis_title=my_ytitle,
        legend_title=my_legend_title,
        height=my_height, width=my_width,
        margin={"r":50, "t":50, "l":50, "b":50},
        autosize=False,
        showlegend=my_showlegend
    )
    return fig
    
#------------------------------------------------------------------------------#
#     Map time zones to states & territoriess, assign region names             #
#------------------------------------------------------------------------------# 
region_dict = {}  
for key in us.states.mapping('name', 'time_zones'):
    value = (
        us.states.mapping('name', 'time_zones')[key][0]
        .replace('America/New_York',        'East')
        .replace('America/Chicago',         'Central')
        .replace('America/Denver',          'Mountain') 
        .replace('America/Los_Angeles',     'Pacific')        
        .replace('America/Anchorage',       'Alaska')
        .replace('Pacific/Honolulu',        'Hawaii') 
        .replace('America/Phoenix',         'Pacific')
        .replace('America/Boise',           'Central')  #for N. Dak, not Idaho
        .replace('America/Puerto_Rico',     'Puerto Rico')
    )
    region_dict[key] = value

#------------------------------------------------------------------------------#
#     Load csv file, process rows and columns                                  #
#------------------------------------------------------------------------------#
file_path = (
    'https://raw.githubusercontent.com/plotly/Figure-Friday/' +
    'main/2024/week-30/rural-investments.csv'
)

investment_data = (
    pl.read_csv(file_path, ignore_errors = True)
    .rename({'State Name': 'state_name'})
    .with_columns(
        pl.col('Investment Dollars').str.replace_all(',', '').cast(pl.Float64),
        state_abbr = pl.col('state_name')
            .replace(us.states.mapping('name', 'abbr')),
        region = pl.col('state_name').replace(region_dict)
    )
    .select(
        'state_name', 'state_abbr', 'County', 
        'County FIPS', 'region','Investment Dollars'
    )
)

investment_data = (
    investment_data
    .with_columns(
        region = pl.when(pl.col('region')
                .is_in(
                    ['Puerto Rico', 'Pacific', 'Central', 'Hawaii', 'Mountain', 'Alaska', 'East']))
        .then('region')
        .otherwise(pl.lit('Others'))
    )
   
)

#------------------------------------------------------------------------------#
#     Create choropleth map                                                    #
#------------------------------------------------------------------------------#
grouped_by_county = (
    investment_data.group_by('state_abbr', 'County', 'County FIPS').sum()
)
geojson_path = 'https://raw.githubusercontent.com/plotly/datasets/master'
geojson_file = 'geojson-counties-fips.json'
fig = px.choropleth(
    grouped_by_county,
    geojson=f'{geojson_path}/{geojson_file}',
    locations='County FIPS',
    color='Investment Dollars',
    color_continuous_scale="Viridis",
    scope="usa",
    title='USA Rural Investment by County - 2024',
    custom_data=['County', 'state_abbr','Investment Dollars']
)

# hover info indented to keep mouse point icon from blocking hover display
fig.update_traces(
    hovertemplate="<br>".join([
        '    %{customdata[0]} County, %{customdata[1]}',
        '   $%{customdata[2]:,d}',
    ])
)

fig =  update_layout(fig, '2024 Rural Investment by USA County', 600, 1200)

annotation = '<b>Data Source:</b> US Department of Agriculture<br><br>'
add_annotation(fig, annotation, 0.8, 1.0, 'left', 'right', 'top')

fig.show()

#------------------------------------------------------------------------------#
#     Create colormap dictionaries for both pareto charts                      #
#------------------------------------------------------------------------------#
# keys are regions, values are colors.  for the 'other' region, hardcode gray
my_colors = px.colors.qualitative.Dark24
my_regions = sorted(investment_data['region'].unique().to_list())
my_color_dict = dict(zip(my_regions, my_colors))
my_color_dict['Others'] = 'gray'

#------------------------------------------------------------------------------#
#     Pareto: data grouped by USA state                                        #
#------------------------------------------------------------------------------#
grouped_by_state = (
    investment_data
    .select('state_name', 'state_abbr', 'Investment Dollars', 'region')
    .group_by('state_name', 'state_abbr', 'region').sum()
    .sort('Investment Dollars', descending=True)
    .with_columns(
        CUM_PCT = 
        (100 * pl.col('Investment Dollars')/pl.col('Investment Dollars').sum())
        .cum_sum()
    )
    .filter(pl.col('state_name').is_in([str(s) for s in list(us.states.STATES)]))
)

fig = px.bar(
    grouped_by_state.sort('Investment Dollars', descending=True),
    x = 'state_abbr',
    y = 'Investment Dollars',
    template='plotly_white',
    color='region',
    color_discrete_map=my_color_dict,
    custom_data=['state_name','Investment Dollars', 'CUM_PCT']
)

# Set y-axes titles
fig.update_yaxes(title_text="<b>primary</b> Investment Dollars", secondary_y=False)
fig.update_yaxes(title_text="<b>secondary</b> Cumulative PCT", secondary_y=True)

# add custom hover information
fig.update_traces(
    hovertemplate="<br>".join([
        '%{customdata[0]}',
        '$%{customdata[1]:,d}',
        'Cumulative: %{customdata[2]:.1f}%',
        '<extra></extra>'
    ])
)
fig = update_layout(
    fig, 'USA Rural Investment by States & Territories - 2024', 600, 1200, 
    my_xtitle= 'State - Abbreviated', 
    my_ytitle= 'Investment (US$)',
    my_legend_title='Region',
    my_showlegend=True
)
fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False)

annotation = '<b>Data Source:</b> US Department of Agriculture<br>'
annotation += 'states & territories are color coded by region<br>'
add_annotation(fig, annotation, 0.5, 0.8, 'left', 'right', 'top')

# chart gets reordered after applying colors, next line restores intended order
fig.update_xaxes(categoryorder='total descending')

fig.show()

#------------------------------------------------------------------------------#
#     Pareto: data grouped by region                                           #
#------------------------------------------------------------------------------#
grouped_by_region = (
    investment_data
    .select('region', 'Investment Dollars')
    .group_by('region').sum()
    .sort('Investment Dollars', descending=True)
    .with_columns(
        CUM_PCT = 
        (100 * pl.col('Investment Dollars')/pl.col('Investment Dollars').sum())
        .cum_sum()
    )
)

fig = px.bar(
    grouped_by_region.sort('Investment Dollars', descending=True),
    'region',
    y = 'Investment Dollars',
    template='plotly_white',
    color='region',
    color_discrete_map=my_color_dict,
    custom_data=['region','Investment Dollars', 'CUM_PCT'],
)

# add custom hover information
fig.update_traces(
    hovertemplate="<br>".join([
        '%{customdata[0]}',
        '$%{customdata[1]:,d}',
        'Cumulative: %{customdata[2]:.1f}%',
        '<extra></extra>'
    ])
)

fig = update_layout(
    fig, 'USA Rural Investment by Region - 2024', 600, 1200,
    my_xtitle='Region',
    my_ytitle='Investment (US$)',
)

fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False)

annotation = '<b>Data Source:</b> US Department of Agriculture<br>'
annotation += 'Regions are appoximations of time zones. Exceptions<br>'
annotation += 'made for states with multiple time zones and for<br>'
annotation += 'states that do not observe daylight saving time.<br><br>'
annotation += "<b>'Others'</b> incudes Virgin Islands, Guam, Samoa & Palua"
add_annotation(fig, annotation, 0.9, 0.8, 'left', 'right', 'top')
fig.show()
