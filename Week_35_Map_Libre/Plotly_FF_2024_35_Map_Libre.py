import polars as pl
import plotly.express as px
import plotly.io as pio
# included next line if using jupyter notebook, commented out if running python
# pio.renderers.default = "notebook_connected"

# us library is a tool for working with US State names and abbreviations
# make a list of valid US states, and filter data with it.
import us
state_list = [s for s in us.states.mapping('abbr', 'name').values()]


def wrap_hover(text, chars_per_line=28):
    '''
    break long hover text into multiple lines, split with html line feeds.
    1st whitespace after chars_per_line value is exceeeded is replaced with <br>
    '''
    result = []
    
    # Counter to track line_Length
    line_length = 0
    
    # Iterate over each character in the text
    for char in text:
        line_length += 1
        if char.isspace():
            if line_length > chars_per_line:
                result.append('<br>')
                line_length = 0
            else:
                result.append(char)
        else:
            result.append(char)
    
    return ''.join(result)

def state_abbr(state):
    '''
    return commonly used abbreviation of full state name, ie New York, NY
    '''
    return us.states.lookup(state).abbr

#------------------------------------------------------------------------------#
#     scan_csv produces polars Lazy frame, with data cleaning flow             #
#------------------------------------------------------------------------------#
data_set = (
    pl.scan_csv('people-map.csv')  # LazyFrame

    .with_columns(pl.col('views_median', 'views_sum').cast(pl.Int32))
    # call wrap_hover function to inserts line feeds
    .with_columns(
        extract_wrap = 
            pl.col('extract')
            .map_elements(wrap_hover, return_dtype=pl.String)
    )
    # add column to count # of times each name appears in the dataset
    .with_columns(
        NAME_COUNT = pl.col('name_clean').count().over('name_clean')
    )

    # Filter out rows with invalid state entries
    .filter(pl.col('state').is_in(state_list))

     # Add column with abbreviated form of each state's name, ie NY for New York   
    .with_columns(
        STATE_ABBR = pl.col('state').map_elements(state_abbr, return_dtype=pl.String)
    )

    # tweak for Washington DC, state
    .with_columns(
        state = pl.when(pl.col('city').str.ends_with('D.C.'))
           .then(pl.lit('D.C.'))
           .otherwise('state')
    )
    
    # tweak for Washington DC, city
    .with_columns(
        city = pl.when(pl.col('city').str.ends_with('D.C.'))
           .then(pl.lit('Washington'))
           .otherwise('city')
    )
    # exclude rows with views_median == 0, or views_sum is null
    .filter(pl.col('views_median') > 0)
    .filter(pl.col('views_sum').is_not_null())
    .with_columns(
        color_size = (1+ pl.col('views_sum').log10() - pl.col('views_sum').log10().min()).pow(2).round(1)
    )
    
    .collect()   # optimize and execute this query, return a regular dataframe
)

#------------------------------------------------------------------------------#
#     scatter_map uses map type 'streets' with Magenta_r sequential colors     #
#------------------------------------------------------------------------------#
fig = px.scatter_map(
    data_set,
    lat='lat',
    lon='lng',
    size='color_size', 
    color='color_size',
    color_continuous_scale='Magenta_r',
    size_max=35,
    zoom=3,
    map_style='streets',
    custom_data=[
        'name_clean',              #  customdata[0]
        'city',                    #  customdata[1]
        'STATE_ABBR',              #  customdata[2]
        'views_median',            #  customdata[3]
        'views_sum',               #  customdata[4]
        'NAME_COUNT',              #  customdata[5]
        'color_size',              #  customdata[6]
        'extract_wrap',            #  customdata[7]
    ],
    range_color=(0,30)  # max log of 7 means 10e6)
)

fig.update_layout(
    autosize=True,
    width=1300,
    height=600,
    margin=dict(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4
    ),
)
#------------------------------------------------------------------------------#
#     Apply hovertemplate                                                      #
#------------------------------------------------------------------------------#
fig.update_traces(
    hovertemplate =
        '<b>%{customdata[0]}: %{customdata[1]}, %{customdata[2]}<br></b>' +
        '<b>Daily Views:</b>%{customdata[3]:>20,}<br>' +
        '<b>Total Views:</b>%{customdata[4]:>20,}<br>' +
        '<b>Name Count: </b>%{customdata[5]:>20}<br>' +   
        '<b>Color/Size Factor: </b>%{customdata[6]:>13}<br><br>' +   
        '%{customdata[7]}<br>' +
        '<extra></extra>'
)

fig.update_layout(
    hoverlabel=dict(
        bgcolor="white",
        font_size=16,
        font_family='courier',  # 'sans-serif mono', # 'courier new', 
    )
)

fig.update_layout(
    margin={"r":0, "t":0, "l":0, "b":0},
    )

fig.write_html(f'Fig_Fri_Week_35_Map.html')
fig.show()

#------------------------------------------------------------------------------#
#     For Data Exploration, histogram of views_sum                             #
#------------------------------------------------------------------------------#
fig = px.histogram(
    data_set, # sqrt()),
    x='views_sum',
    nbins=1000
)
fig.update_layout(width=800, height=400, template='plotly_white')
fig.show()

#------------------------------------------------------------------------------#
#     For Data Exploration, histogram of NAME_COUNT                            #
#------------------------------------------------------------------------------#
fig = px.histogram(
    data_set, # sqrt()),
    x='NAME_COUNT',
)
fig.update_layout(width=800, height=400, template='plotly_white')
fig.show()

#------------------------------------------------------------------------------#
#     Show min and max values of NAME_COUNT, SIZE                              #
#------------------------------------------------------------------------------#

print(
    data_set
    .select(pl.col('name_clean', 'views_sum', 'color_size'))
    .sort('views_sum', descending=False)
    .filter(pl.col('name_clean').is_in(['Barack Obama', 'Charles R. Gleason']))
    .unique('name_clean')
)
