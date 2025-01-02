'''
Plotly Figure Friday - 2024 week 52 - stocks 
58 out of 172 SaaS (Software as a Service) companies in this dataset (34%) are
based in the San Francisco Bay Area, a region that includes San Jose, Silicon 
Valley, Palo Alto and Berkeley. This script uses map Libre to map the locations 
of the SF Bay Area companies, with useful hover and marker sized by Market Cap.
'''
import plotly.express as px
import polars as pl
currency_to_floats = ['Stock Price']
currency_to_mils = [
    'Market Cap', 'Last Quarter Revenue','Annualized Revenue',
    'Last Quarter EBITDA','Annualized EBITDA','Last Quarter Net Income',
    'Annualized Net Income','Cash and Short Term Investments'
    ]

def wrap_hover(text, CHARS_PER_LINE=45):
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
            if line_length > CHARS_PER_LINE:
                result.append('<br>')
                line_length = 0
            else:
                result.append(char)
        else:
            result.append(char)
    
    return ''.join(result)


df_norcal = (
    pl.scan_csv('SaaS-businesses-NYSE-NASDAQ.csv')  # scan_csv --> lazy frame
    .filter(pl.col('Headquarters').str.contains('California'))
    .with_columns(
        Headquarters = 
            pl.col('Headquarters')
            .str.split(by =',')
            .list.slice(0,1)
            .list.first()
    )
    .filter(    # exclude 4 cities on the list from southern california
        ~pl.col('Headquarters')
        .is_in(['Glendale', 'San Diego', 'Santa Barbara','Ventura'])
    )
    # remove Orcacle, and Snowflake - they have moved out of the Bay Area
    .filter(~pl.col('Company').str.contains('Oracle'))
    .filter(~pl.col('Company').str.contains('Snowflake'))

    # convert dollars as strings to millions of dollars as floats
    .with_columns(pl.col(currency_to_mils).str.replace_all(',', ''))
    .with_columns(pl.col(currency_to_mils).str.replace('$', '', literal=True))
    .with_columns(pl.col(currency_to_mils).cast(pl.Float64))
    .with_columns(pl.col(currency_to_mils)/1000000)
    .with_columns(pl.col(currency_to_mils).round(0))

    # convert dollars as strings to floats
    .with_columns(pl.col(currency_to_floats).str.replace_all(',', ''))
    .with_columns(pl.col(currency_to_floats).str.replace('$', '', literal=True))
    .with_columns(pl.col(currency_to_floats).cast(pl.Float64))
    .with_columns(pl.col(currency_to_floats).round(2))

    # create a categorical column based on Market Cap
    .with_columns(MKT_CAP_CAT = pl.lit('')) # intialize
    .with_columns(
        MKT_CAP_CAT = 
            pl.when(pl.col('Market Cap')>100e3).then(pl.lit('> $100B'))
              .when(pl.col('Market Cap')>10e3).then(pl.lit('> $10B'))
              .when(pl.col('Market Cap')>1e3).then(pl.lit('> $1B'))
              .when(pl.col('Market Cap')>100).then(pl.lit('> $100M'))
              .when(pl.col('Market Cap')>10).then(pl.lit('> $10M'))
              .when(pl.col('Market Cap')>1).then(pl.lit('> $1M'))
    )
    .with_columns(
        MARKER_SIZE = 
            pl.when(pl.col('Market Cap')>100e3).then(pl.lit(6))
              .when(pl.col('Market Cap')>10e3).then(pl.lit(5))
              .when(pl.col('Market Cap')>1e3).then(pl.lit(4))
              .when(pl.col('Market Cap')>100).then(pl.lit(3))
              .when(pl.col('Market Cap')>10).then(pl.lit(2))
              .when(pl.col('Market Cap')>1).then(pl.lit(1))
    )
    .with_columns(
        PROD_DESC_WRAP = 
            pl.col('Product Description')
            .map_elements(wrap_hover, return_dtype=pl.String)
    )
    .drop('Company Website', 
          'Company Investor Relations Page',
          'Lead Investor(s) Pre-IPO',
          'S-1 Filing',
          'September 2024 Website Traffic (Estimate)',
          'YoY Change in Website Traffic%',
          '2023 10-K Filing',
          'Product Description'
    )
    .collect()      # collect uses lazy frame to make polars dataframe
)
for c in currency_to_mils:
    df_norcal = df_norcal.rename({c: c + ' [M$]'})

#-------------------------------------------------------------------------------
#    Load file with GPS coordinates -- this data is from ChatGPT 
#-------------------------------------------------------------------------------
df_coords =(
    pl.scan_csv('df_norcal_coords.csv')
    # remove degree notation from Long, Lat strings before convert to float
    .with_columns(pl.col(['Long', 'Lat']).str.replace('°N', ''))
    .with_columns(pl.col(['Long', 'Lat']).str.replace('°W', '')) 
    .with_columns(pl.col(['Long', 'Lat']).cast(pl.Float64))
    # For Longitude, have to multiply by -1 to get degrees west
    .with_columns(pl.col(['Long'])*-1.0)
    .collect()
)
#-------------------------------------------------------------------------------
#    Join GPS coordinates with main dataset 
#-------------------------------------------------------------------------------
df = (
    df_norcal
    .join(
        df_coords,
        how = 'left',
        on='Company'
    )
)
df_cols = df.columns
left_cols = [c for c in df_cols if c not in ['PROD_DESC_WRAP', 'Founder(s)']]
right_cols = ['PROD_DESC_WRAP', 'Founder(s)']
df = df.select(pl.col(left_cols  + right_cols))

#-------------------------------------------------------------------------------
#    Make a GPS Scatter Plot of Bay Area Companies 
#-------------------------------------------------------------------------------
#define marker colors based on Market Cap
dict_marker_map = {
    '> $1M': px.colors.qualitative.Set1[0],
    '> $10M':  px.colors.qualitative.Set1[5],
    '> $100M': px.colors.qualitative.Set1[4],
    '> $1B':  px.colors.qualitative.Set1[3],
    '> $10B':px.colors.qualitative.Set1[1],
    '> $100B':px.colors.qualitative.Set1[2],
}
fig = px.scatter_map(
    df.sort('Market Cap [M$]', descending=True),
    lat='Lat',
    lon='Long',
    height=1000, width=800,
    size='MARKER_SIZE', # 'Market Cap [M$]', 
    color='MKT_CAP_CAT',
    color_discrete_map= dict_marker_map,
    zoom=9,
    map_style='carto-voyager', #open-street-map',  # 'streets',
    custom_data=[
        'Company',             #  customdata[0]
        'Year Founded',        #  customdata[1]
        'IPO Year',            #  customdata[2]
        'Headquarters',        #  customdata[3]
        'Market Cap [M$]',     #  customdata[4]
        'PROD_DESC_WRAP',      #  customdata[5]
    ],
    title='SaaS Companies in San Francisco Bay Area/Silicon Valley'
)
#------------------------------------------------------------------------------#
#     Apply hovertemplate                                                      #
#------------------------------------------------------------------------------#
fig.update_traces(
    hovertemplate =
        '<b>%{customdata[0]}' +
        ' (Founded: %{customdata[1]},  ' +
        'IPO: %{customdata[2]})</b><br>' +
        '%{customdata[3]}<br>' +
        'Market Cap : $%{customdata[4]:,}M<br>' +
        'Products : %{customdata[5]}<br>' +
        '<extra></extra>'
)

fig.update_layout(
    hoverlabel=dict(
        bgcolor="white",
        font_size=16,
        font_family='arial',  # 'sans-serif mono', # 'courier new', 
    ),
    legend_title='Market Cap'
)

fig.show()