import plotly.express as px
import polars as pl

#-------------------------------------------------------------------------------
#    Read excel speadsheet to polars dataframe, df_source.
#-------------------------------------------------------------------------------
df_source = list_col_names = (
    pl.read_excel(
        'CMO-Historical-Data-Monthly.xlsx',
        sheet_name='Monthly Prices',
        has_header=True,
        read_options={'header_row': 4}
    )
    .rename({'__UNNAMED__0': 'MONTH'})
)
#-------------------------------------------------------------------------------
#    Make a list of useful columns names using rows 7 & 6
#-------------------------------------------------------------------------------
df_header_info = (
    df_source   
    .head(2)
    .transpose(include_header=True)
    .rename({
        'column'         : 'DESC',
        'column_0'       : 'UNITS',
        'column_1'       : 'ITEM',
        })
    .with_columns(
         column_names = # if units column is blank, just take name without units
             pl.when(pl.col('ITEM').is_not_null())
               .then(pl.col('ITEM') + pl.lit(' ') + pl.col('UNITS'))
               .otherwise(pl.col('DESC'))
    )
)
list_column_names = (
    df_header_info
    .select(pl.col('column_names'))
    .to_series().to_list()
)

#-------------------------------------------------------------------------------
#    Make a working dataframe, convert data columns from  strings to floats
#-------------------------------------------------------------------------------
                                                                               
data_col_names = df_source.columns[1:]

df = (
    df_source
    .with_row_index()
    .filter(pl.col('index') > 1)
    .drop('index')
    .with_columns(pl.col(data_col_names).cast(pl.Float64, strict=False))
    .with_columns(YEAR_STR = pl.col('MONTH').str.slice(0,4))
    .with_columns(MONTH_STR = pl.col('MONTH').str.slice(-2,2))
    .with_columns(DATE_STR = (pl.col('YEAR_STR') + '-' + pl.col('MONTH_STR')))
    .with_columns(DATE = pl.col('DATE_STR').str.to_date('%Y-%m'))
    .drop(['YEAR_STR', 'MONTH_STR', 'DATE_STR'])  # temporary cols to make DATE

)
# rename all columns, and add newly added DATE column to this list
df.columns=list_column_names + ['DATE']
# reorder the columns, with DATE at the far left, followed by data columns
df = (
    df
    .select(['DATE'] + list_column_names)
    .drop('MONTH')
)

#-------------------------------------------------------------------------------
#    use px line to plot commodoty prices - Coffee or Tea?
#-------------------------------------------------------------------------------
df_plotting = (   # add average coffee price
    df
    .with_columns(
        (   # calculate average coffee prices, AVG_TEA included in dataset
            (pl.col('COFFEE_ARABIC ($/kg)') + 
             pl.col('COFFEE_ROBUS ($/kg)'))
            /2.0).alias('COFFEE_AVG ($/kg)')
        )
)
fig = px.line(
    df_plotting,
    'DATE',
    ['TEA_AVG ($/kg)', 'COFFEE_AVG ($/kg)'],
    template='simple_white',
    height=400, width=800
)

#-------------------------------------------------------------------------------
#    Touch up and annotate
#-------------------------------------------------------------------------------
tea_color = '#006400'
coffee_color = '#D2691E'
fig['data'][0]['line']['color']=tea_color
fig['data'][1]['line']['color']=coffee_color
date_newest = df_plotting['DATE'].to_list()[-1]
coffee_newest = df_plotting['COFFEE_AVG ($/kg)'].to_list()[-1]
tea_newest = df_plotting['TEA_AVG ($/kg)'].to_list()[-1]
offset = 10
fig.add_annotation(
    x=date_newest, xanchor='left',
    y=coffee_newest, 
    showarrow=False,
    text='<b>COFFEE</b>',
    align='right',
    font=dict(size=14, color=coffee_color),
)
fig.add_annotation(
    x=date_newest, xanchor='left',
    y=tea_newest, 
    showarrow=False,
    text='<b>TEA</b>',
    font=dict(size=14, color=tea_color),
)
fig.update_layout(
    showlegend=False,
    title_text = (
        'COFFEE OR TEA?<br>' +
        '<sup>Coffee jitter is not just biological</sup>'
        ),
    xaxis_title='', yaxis_title='AVERAGE PRICE ($/kg)'
    )

fig.show()