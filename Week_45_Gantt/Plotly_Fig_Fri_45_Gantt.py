from datetime import datetime
import polars as pl
import plotly.express as px

# constants
MIN_YEARS = 25  # gantt chart includes mines with MIN_YEARS or more of service
SOURCE_LOCAL = False # if True, data from csv, if False data from get git-repo
today = datetime.now().strftime('%Y_%m_%d')
local_csv = 'week_45_data.csv'

#------------------------------------------------------------------------------#
#     initialize dataframe df_source from local file or git repo
#------------------------------------------------------------------------------#
if SOURCE_LOCAL:
    # this path reads data previously saved to local drive
    df_source = (
        pl.scan_csv(local_csv, ignore_errors=True)
        .collect()
    )
else:
    # this path downloads the data from an external git repository
    web_csv = (  # file name split over 2 lines, PEP-8
        'https://raw.githubusercontent.com/plotly/Figure-Friday/refs/heads/' +
        'main/2024/week-45/mines-of-Canada-1950-2022.csv'
    )
    df_source = (
        pl.read_csv(web_csv,ignore_errors=True)
        .filter(pl.col('close1').str.to_uppercase() != 'OPEN')
        .filter(pl.col('commodityall').str.contains('Coal'))
        .rename(
            {   # clean up selected column names
                'company1' : 'COMPANY',
                'namemine' : 'MINE',
                'town'     : 'TOWN',
                'province' : 'PROVINCE',
                'open1'    : 'YEAR_OPENED',
                'close1'   : 'YEAR_CLOSED'
            }
        )
        .with_columns(pl.col('YEAR_OPENED', 'YEAR_CLOSED').cast(pl.Int16))
    )
    # data has been read from git-repo, so save a local copy
    df_source.write_csv(local_csv)

#------------------------------------------------------------------------------#
#     add DATE_OPENED and DATE_CLOSED as Date columns, needed for timeline 
#------------------------------------------------------------------------------#
df = (
    df_source
    .with_columns(
        DATE_OPENED = pl.col('YEAR_OPENED')
                        .cast(pl.String)
                        .str.strptime(pl.Date, format='%Y')
    )
    .with_columns(
        DATE_CLOSED = pl.col('YEAR_CLOSED')
                        .cast(pl.String)
                        .str.strptime(pl.Date, format='%Y')
    )
    .select(pl.col('COMPANY', 'MINE', 'TOWN', 'PROVINCE',
               'DATE_OPENED', 'DATE_CLOSED', 'YEAR_OPENED', 'YEAR_CLOSED')
            )

    .sort(['PROVINCE', 'DATE_OPENED'])
)

#------------------------------------------------------------------------------#
#     Use province names as section titles, indexed with integer-like values,
#     1, 2, 3, etc. S. Section members are coal mines, with incremental index 
#     values of 1.01, 1.02, etc. Each province group has a first row that will
#     be formatted as a section head  
#------------------------------------------------------------------------------#
province_list = sorted(list(set(df['PROVINCE'])))
# Add header row above the data of each province
df_list = []
for i, province in enumerate(province_list):
    opened = ( # 1st row, opened is the earliest year opened
        df     # of all coal mines in this province
        .filter(pl.col('PROVINCE') == province)
        .select(pl.col('DATE_OPENED'))
        .min().to_series()[0]
    )
    closed = (  # closed is the last year closed of all coal mines
        df      # in this province
        .filter(pl.col('PROVINCE') == province)
        .select(pl.col('DATE_CLOSED'))
        .max().to_series()[0]
    )
    df_first_row = (   # here is the data frame for 1st row of province group
        pl.DataFrame(
            {
                'COMPANY'      : '<b>' + province.upper() + '</b>',
                'MINE'         : '',
                'TOWN'         : '',
                'PROVINCE'     : province,
                'DATE_OPENED'  : opened,
                'DATE_CLOSED'  : closed,
                'YEAR_OPENED'  : opened.year,
                'YEAR_CLOSED'  : closed.year,
            }
        )
        .with_columns(pl.col('YEAR_OPENED', 'YEAR_CLOSED').cast(pl.Int16))
    )
    df_province = ( # finsh province group with concat of 1st row and all others
        pl.concat(
            [
                df_first_row,
                df
                .filter(pl.col('PROVINCE') == province)
                .sort('DATE_OPENED', descending=False)
            ]
        )
        # temporary columns GROUP, GROUP_COUNT used for calculating item #
        .with_columns(GROUP = pl.lit(i+1))
        .with_columns(GROUP_COUNT = pl.col('GROUP').cum_count().over('GROUP') - 1)
        .with_columns(ITEM = (   # ITEM serves a row index
            pl.col('GROUP') + pl.col('GROUP_COUNT')/100.0).cast(pl.Float32()))
        .with_columns(
            ITEM_COMPANY = (
                pl.lit('  ')  +  
                pl.col('ITEM').cast(pl.Utf8).str.pad_end(4, '0')
                + pl.lit(': ') 
                + pl.col('COMPANY')
            ),
        )
        .with_columns(
            YEAR_OPENED = (pl.col('DATE_OPENED').dt.year().cast(pl.Int32)),
            YEAR_CLOSED = (pl.col('DATE_CLOSED').dt.year().cast(pl.Int32))                                      
        )
        .with_columns(
            DURATION_YEARS = (pl.col('YEAR_CLOSED') - pl.col('YEAR_OPENED'))
        )
        .with_columns(
            MINE = pl.when(pl.col('MINE').is_null())
                     .then(pl.lit('None'))
                     .otherwise('MINE')
        )
        .with_columns(
            TOWN = pl.when(pl.col('TOWN').is_null())
                     .then(pl.lit('No Name Town'))
                     .otherwise('TOWN')
        )
        .filter(pl.col('DURATION_YEARS') >= MIN_YEARS)
    )
    # provinces need 2 or more rows, as first row is only a header
    if len(df_province) > 1:
        df_list.append(df_province)

df = pl.concat(df_list)  # this is the final step of data frame creation

#------------------------------------------------------------------------------#
#     plolty timeline
#------------------------------------------------------------------------------#
my_title = 'Shuttered Canadien Coal Mines<br>'
my_title += f'<sup>Closed mines that operated for {MIN_YEARS}+ years'
fig = px.timeline(   # DATE_OPENED AND DATE_CLOSED are type Date
    df,
    x_start='DATE_OPENED',
    x_end='DATE_CLOSED',
    y = 'ITEM_COMPANY',   # index has been prepended to COMPANY for sorting
    title = my_title,
    height = 1400,
    width = 1000,
    color='GROUP_COUNT',
    custom_data=['COMPANY', 'TOWN', 'PROVINCE',  'MINE', 
                 'YEAR_OPENED', 'YEAR_CLOSED', 'DURATION_YEARS']
)

fig.update_yaxes(categoryorder='category descending', automargin=True)
fig.update_layout(
    title=dict(font=dict(size=24), automargin=False, yref='paper'))
fig.update_layout(
    yaxis = dict( tickfont = dict(size=16), tickmode = 'linear', dtick=0.01))
fig.update_layout(xaxis = dict( tickfont = dict(size=16)))

fig.update_layout(
    xaxis={'side': 'top'}, 
    yaxis={'side': 'right'},
    template = 'presentation',
    yaxis_title = '',
)

#------------------------------------------------------------------------------#
#     Use list comprehension to find integer-like ITEM#s to use as group heads
#------------------------------------------------------------------------------#
int_items = [
    i for i, x in enumerate(df['ITEM'].sort(descending=True).to_list()) 
    if x == round(x, 0)
]
for item_num in int_items:  # put thick horiz line on province group head
    fig.add_hline(
        y=item_num, 
        line_width=10, 
        line_color='black', 
        layer='below'
        )

#------------------------------------------------------------------------------#
#     Add vertical line on today's date. Useful when using timeline for project
#     schedule, not useful in this case where date resolution is by year. x is
#     number of milliseconds since epoch, used as a workaround
#------------------------------------------------------------------------------#
fig.add_vline(
    x=datetime.strptime(today, "%Y_%m_%d").timestamp() * 1000,
    line_width=2, 
    line_color="green", 
    line_dash="dash", 
    annotation_text=str(datetime.now().strftime('%b %d')),
    annotation_position='bottom',
    annotation_font_size=20
)
#------------------------------------------------------------------------------#
#     customize hover template, uses columns from px.timeline, custom_data
#------------------------------------------------------------------------------#
fig.update_traces(
    hovertemplate="<br>".join([
        '<b>%{customdata[0]}</b>',
        '%{customdata[1]}, %{customdata[2]}',
        'Mine Name: %{customdata[3]}',
        '%{customdata[4]} to %{customdata[5]}',
        '(%{customdata[6]:.0f} Years)',
        '<extra></extra>'
    ])
)

fig.update_layout(
    hoverlabel=dict(font=dict(family='sans-serif', size=16)),
    showlegend=False,
    title_x=0
    )

#------------------------------------------------------------------------------#
#     fix y labels, for example '1.03 - Mine XYZ' becomes 'Mine XYZ'
#------------------------------------------------------------------------------#
y_ticks = df['ITEM_COMPANY']
fig.update_yaxes(
    tickmode='array',
    tickvals=y_ticks,
    ticktext=[y[7:] for y in y_ticks]  # strips away first 7 characters
)

fig.show()
fig.write_html('Shuttered_Coal_Mines.html')
