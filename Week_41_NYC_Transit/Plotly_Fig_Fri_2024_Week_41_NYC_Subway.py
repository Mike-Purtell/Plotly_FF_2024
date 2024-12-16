import polars as pl
import plotly.express as px
import polars.selectors as cs

def plot_by_year(
        df, 
        rolling_mean=0, 
        title='no title given',
        annotate_text = 'NONE',
        annotate_x=0,
        annotate_y=0,
        ):
    ''' Function to make px.line by year with custom annotation'''
    if rolling_mean:
        df = (
            df
            .select(
                pl.col('DATE', '2020','2021', '2022', '2023', '2024')
            )
            .with_columns(
                pl.col(['2020','2021', '2022', '2023', '2024'])
            .rolling_mean(window_size=7)
            )
        )

    fig=px.line(
        (
            df
            .select(
                pl.col('DATE', '2020','2021', '2022', '2023', '2024')
            )
         ),
        'DATE',
        ['2024','2023', '2022', '2021', '2020'],
        template='simple_white',
        height=600,
        width=900
    )
    fig.update_layout(
        title=title,
        yaxis_tickformat='.0%',
        legend_title_text='YEAR',
        yaxis_range = [0, 1.5],
    )
    fig.update_xaxes(title='')
    fig.update_yaxes(title='RIDERSHIP % RELATIVE TO PRE-PANDEMIC')
    fig.update_traces(hovertemplate=None)
    fig.update_layout(hovermode="x unified")

    # add annotation if x or y value are non-zero, otherwise skip
    if (annotate_x + annotate_y > 0.0):
        fig.add_annotation(
            x=annotate_x, xref='paper',
            y=annotate_y, yref='paper',
            text=annotate_text,
            showarrow=True,
            xanchor='left',
            yanchor='top',
            align='left',
            font=dict(size=14)
        )
        fig.update_annotations(showarrow=False)
    return fig


df_all = (
    pl.scan_csv('MTA_Daily_Ridership_Data__Beginning_2020.csv',
    )
    .with_columns(
        DATE = pl.col('Date').str.to_datetime('%m/%d/%Y')
    )
    .with_columns(MONTH_NUM = pl.col('DATE').dt.strftime('%m').cast(pl.UInt8))
    .with_columns(MONTH_NAME = pl.col('DATE').dt.strftime('%b'))
    .with_columns(DAY = pl.col('DATE').dt.strftime('%d').cast(pl.UInt8))
    .with_columns(YEAR = pl.col('DATE').dt.strftime('%Y').cast(pl.UInt16))
    .with_columns(DAY_NAME = pl.col('DATE').dt.strftime('%a'))
    .rename(
        {
            'Subways: Total Estimated Ridership'                        : 'SUB_RIDERS',
            'Subways: % of Comparable Pre-Pandemic Day'                 : 'SUB_PCT',
            'Buses: Total Estimated Ridership'                          : 'BUS_RIDERS',
            'Buses: % of Comparable Pre-Pandemic Day'                   : 'BUS_PCT',
            'Metro-North: Total Estimated Ridership'                    : 'METRO_N_RIDERS',
            'Metro-North: % of Comparable Pre-Pandemic Day'             : 'METRO_N_PCT',
            'LIRR: Total Estimated Ridership'                           : 'LIRR_RIDERS',
            'LIRR: % of Comparable Pre-Pandemic Day'                    : 'LIRR_PCT',
            'Access-A-Ride: Total Scheduled Trips'                      : 'ACCESS_RIDERS',
            'Access-A-Ride: % of Comparable Pre-Pandemic Day'           : 'ACCESS_PCT',
            'Bridges and Tunnels: Total Traffic'                        : 'BT_TRAFFIC',
            'Bridges and Tunnels: % of Comparable Pre-Pandemic Day'     : 'BT_PCT',
            'Staten Island Railway: Total Estimated Ridership'          : 'SI_RW_RIDERS',
            'Staten Island Railway: % of Comparable Pre-Pandemic Day'   : 'SI_RW_PCT',
        }
    )
    .with_columns(cs.ends_with('_PCT').cast(pl.Float32)/100)
    .select(
        pl.col(
            'DATE', 'MONTH_NUM', 'MONTH_NAME', 'DAY', 'DAY_NAME', 'YEAR', 
            'SUB_RIDERS', 'SUB_PCT', 'BUS_RIDERS', 'BUS_PCT', 'LIRR_RIDERS', 
            'LIRR_PCT', 'METRO_N_RIDERS', 'METRO_N_PCT', 'ACCESS_RIDERS', 'ACCESS_PCT', 
            'BT_TRAFFIC', 'BT_PCT', 'SI_RW_RIDERS', 'SI_RW_PCT'
            )
    )
    .collect()
)

df_subway = (
    df_all
    .select(pl.col('DATE','MONTH_NUM', 'MONTH_NAME', 'DAY', 'DAY_NAME', 'YEAR', 'SUB_RIDERS', 'SUB_PCT'))
    .pivot(
        on='YEAR',
        index=['MONTH_NUM', 'MONTH_NAME','DAY'],
        values='SUB_PCT'
    )
    .with_columns(DATE = pl.col('MONTH_NAME') + pl.lit(' ') + pl.col('DAY').cast(pl.String))
    .sort('MONTH_NUM', 'DAY')
)

#
#   Plot Subway ridership by year, relative to pre-pandemic using raw data,
#   without smoothing
#
ann_txt = 'Raw data shows weekday & weekend patterns,<BR>'
ann_txt += "with notable peaks on Martin Luther King Day (Jan 20),<br>"
ann_txt += "Presidents Day (Feb 20), and the biggest by far on Veteran's Day (Nov 11)"
fig = plot_by_year(
    df_subway, 
    rolling_mean = 0, 
    title='NYC SUBWAY RIDERSHIP, RELATIVE TO PRE-PANDEMIC',
    annotate_text = ann_txt,
    annotate_x=0.05,
    annotate_y=0.98
)
fig.write_html('Subway_PCT_Pre_Pandemic.html')
fig.show()
#
#   Plot Subway ridership by year, relative to pre-pandemic.
#   Use rolling mean of 7-days for smoothing
#
ann_txt = 'Rolling mean of 7 minimizes variation by weekday, and stretches the 1-day holiday peaks,<BR>'
ann_txt += 'across 7 days. Ridership increases from 2020 to 2023 appear to be leveling off.<br><br>'
ann_txt += '<span style="color: MediumBlue"><b>Going forward, does it make sense to put more focus on year-over-year trends,<br>'
ann_txt += 'with less focus on pre-pandemic comparisons?</b></span>'

fig = plot_by_year(
    df_subway, 
    rolling_mean = 7, 
    title='NYC SUBWAY RIDERSHIP, RELATIVE TO PRE-PANDEMIC, ROLLING MEAN =7',
    annotate_text = ann_txt,
    annotate_x=0.05,
    annotate_y=0.98
)
fig.write_html('Subway_PCT_Pre_Pandemic_7_Day_Rolling.html')
fig.show()

#
#  Make a simple dataframe to may day number to day name, used in later join
#
df_day_num = (
    pl.DataFrame(
        {
            'DAY_NAME': ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
            'DAY_NUM' : [i for i in range(1,8,1)]
        }
    )
)
df_day_num
#
#  Make dataframe to show ridership levels by day of week. Use interquartile method
#  to identify outliers, and then remove them
#
df_subway_day = (
    df_all
    .select(pl.col('MONTH_NUM', 'MONTH_NAME', 'DAY', 'DAY_NAME', 'YEAR', 'SUB_RIDERS', 'SUB_PCT'))
    .join(
        df_day_num,
        on = 'DAY_NAME',
        how='right'
    )
    .with_columns(
        Q1= pl.col('SUB_RIDERS').quantile(0.25).over(['YEAR', 'DAY_NAME']),
        Q3= pl.col('SUB_RIDERS').quantile(0.75).over(['YEAR', 'DAY_NAME']),
    )
    .with_columns(
        IQR= (pl.col('Q3')-pl.col('Q1')),
    )
    .with_columns(
        OUTLIER_L = pl.col('Q1') - (1.5 * pl.col('IQR')),
        OUTLIER_H = pl.col('Q3') + (1.5 * pl.col('IQR')),
    )
    .with_columns(
        IS_OUTLIER = (
            (pl.col('SUB_RIDERS') > pl.col('OUTLIER_H'))
            |
            (pl.col('SUB_RIDERS') < pl.col('OUTLIER_L'))
        ).cast(pl.Boolean)
    )
    .filter(pl.col('IS_OUTLIER') == False)
    .sort('YEAR', 'MONTH_NUM', 'DAY')
    .group_by(['YEAR', 'DAY_NAME', 'DAY_NUM'])
    .agg(pl.col('SUB_RIDERS').mean())
    .pivot(
        on='YEAR',
        index=['DAY_NAME', 'DAY_NUM'],
        values='SUB_RIDERS'
    )
    .sort('DAY_NUM')
    .select(pl.col('DAY_NAME', 'DAY_NUM','2024', '2023','2022', '2021','2020'))
    .with_columns(((pl.col('2020', '2021','2022', '2023','2024')/1000000)).cast(pl.Float32))
)

#
#  Plot ridership levels by day of week
#
fig=px.scatter(
    df_subway_day,
    'DAY_NAME',
    df_subway_day.columns[2:],
    template='simple_white',
    height=600,
    width=900,
)
fig.update_layout(
    title='NYC SUBWAY RIDERSHIP AVERAGE BY DAY OF WEEK<br><sup>OUTLIERS EXCLUDED</sup>',
    # yaxis_tickformat='.0%',
    legend_title_text='YEAR',
    yaxis_range = [0, 5],

)
fig.update_xaxes(title='')
fig.update_yaxes(title='AVERAGE RIDERSHIP [Million]')
fig.update_traces( mode = 'lines+markers', hovertemplate=None)
fig.update_layout(hovermode="x unified")

ann_txt = 'Fewer subway riders on weekends, not surprising.<BR>'
ann_txt += 'Monday through Friday rider counts were flatter during early years of the pandemic,<br>'
ann_txt += 'noticable Monday & Friday drop-off in recent years.<br>'
ann_txt += '<span style="color: MediumBlue"><b>When did the Figure Friday program start?<br>'

#ann_txt += '<span style="color: MediumBlue"><b>Going forward, does it make sense to put more focus on year-over-year trends,<br>'
#ann_txt += 'with less focus on pre-pandemic comparisons?</b></span>'
fig.add_annotation(
    x=0.05, xref='paper',
    y=0.98, yref='paper',
    text=ann_txt,
    showarrow=True,
    xanchor='left',
    yanchor='top',
    align='left',
    font=dict(size=14)
)
fig.update_traces(hovertemplate="%{y:.2f} M")
fig.update_annotations(showarrow=False)
fig.write_html('Subway_Weekday_Patterns.html')
fig.show()
