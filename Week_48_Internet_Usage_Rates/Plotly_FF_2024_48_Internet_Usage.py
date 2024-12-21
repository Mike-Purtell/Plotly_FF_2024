import plotly.express as px
import polars as pl
import polars.selectors as cs
#-------------------------------------------------------------------------------
#   Read data set, drop columns with uniform data
#-------------------------------------------------------------------------------
df = (
    pl.read_csv('API_IT.NET.USER.ZS_DS2_en_csv_v2_2160.csv')
    .drop('Indicator Name', 'Indicator Code')
)

#-------------------------------------------------------------------------------
#   data has years as columns, countrys as rows. Transpose, then cast YEAR as
#   integer, and percentage columns as type float. Drop all years prior to 1990
#-------------------------------------------------------------------------------
df_transpose = (
    df
    .drop('Country Code')
    .transpose(column_names='Country Name', include_header=True, header_name='YEAR')
    .with_columns(pl.col('YEAR').cast(pl.UInt16))
    .with_columns(cs.string().cast(pl.Float32))
    .filter(pl.col('YEAR') >= 1990)
)

#-------------------------------------------------------------------------------
#   First level filter drops countries where all values are null
#-------------------------------------------------------------------------------
df_transpose = (
    df_transpose[
        [s.name for s in df_transpose 
            if not (s.null_count() == df_transpose.height)
            ]
        ]
)

#-------------------------------------------------------------------------------
#   Next step is to calculate year over year % increase with polars .diff
#   Filter out countries where diff values are all null, and filter out 
#   countries that where the max year over year increase is < 25%
#-------------------------------------------------------------------------------
country_list = df_transpose.select(cs.float()).columns
df_diff = df_transpose
for country in country_list:
    df_diff = (
        df_diff
        .rename({country: country+'_ORG'})
        .with_columns(pl.col(country+'_ORG').diff(n=1).alias(country))
    )
    drop_this_country = False
    if df_diff.select(pl.col(country)).is_empty():
        drop_this_country = True
    else:
        if df_diff[country].max() < 25:
            drop_this_country = True
    if drop_this_country:
        df_diff = df_diff.drop(country, country+'_ORG')                 
data_col_list =  sorted(df_diff.select(cs.float()).columns)
df_diff = df_diff.select(['YEAR'] + data_col_list)

#-------------------------------------------------------------------------------
#   Prepare dataframe for plotting, use px.line() with Year as X-Axis
#-------------------------------------------------------------------------------

rename_col_list =  sorted(df_diff.drop('YEAR').select(~cs.ends_with('_ORG')).columns)
df_plot = df_diff.select(['YEAR'] + rename_col_list)
plot_col_list = df_plot.select(pl.all().exclude('YEAR')).columns    
fig=px.line(
    df_plot,
    'YEAR',
    plot_col_list,
    template='simple_white',
    width=800, height=400,
    title=(
        'Internet Percent Change, Year over Year<br>' +
        '<sup>Only countries who reached 25% growth for any year</sup>'
    )
)
fig.update_layout(
    xaxis=dict(title=dict(text='YEAR')),
    yaxis=dict(title=dict(text='% CHANGE')
    ),
    legend=dict(title=dict(text='Country')),
)

fig.show()
