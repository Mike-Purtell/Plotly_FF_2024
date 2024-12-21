import plotly.express as px
import polars as pl
import pandas as pd   # pandas once used for reading table from url

new_england_states = [
    'Connecticut','Maine', 'Massachusetts',
    'New Hampshire',  'Rhode Island','Vermont', 
]
# make dataframe of population for New England States for data normalization
url = 'https://worldpopulationreview.com/states'
df_pop = (
    pl.from_pandas(pd.read_html(url)[0])    # pandas
    .filter(pl.col('State').is_in(new_england_states))
    .rename({'2024 Pop.': 'POP'})
    .select('State', 'POP')
)

def get_fig(df, x_param, my_custom_data = []):
    fig = (
        px.line(
            df,
            x=x_param,
            y=new_england_states,
            template='simple_white',
            height=400, width=800,
            line_shape='spline',  # I learned this during Fig_Fri_48 Zoom Call,
            custom_data=my_custom_data
        )
    )
    # only use x_label of x_param is WEEK_NUM, all other are obvious
    x_label = x_param if x_param=='WEEK_NUM' else ''

    if x_param == 'DATE':
        fig.update_xaxes(
            dtick="M1",
            tickformat="%b\n%Y",
            ticklabelmode="period")
    elif x_param == 'HOUR':
        fig.update_xaxes(
            dtick="H1",
            ticklabelmode='period'
        )
    elif x_param == 'WEEK_NUM':
        fig.update_xaxes(
            dtick='3',
            ticklabelmode='period'
        )
    fig.update_layout(
        title=(
            f'2024 New England Electricity Demand by {x_param}'.upper() +
            '<br><sup>Missing Feb 6 through Feb 17</sup>'
        ),
        yaxis_title='KWatt Hours per Resident'.upper(),
        xaxis_title = x_label,
        legend_title='STATE',
    )
    return fig

#-------------------------------------------------------------------------------
#   Read data set, and clean up
#-------------------------------------------------------------------------------
def tweak():
    return (
        pl.scan_csv('megawatt_demand_2024.csv')   # scan_csv returns a Lazyframe
        .rename(
            {
            'Connecticut Actual Load (MW)'                      : 'Connecticut',
            'Maine Actual Load (MW)'                            : 'Maine',
            'New Hampshire Actual Load (MW)'                    : 'New Hampshire',
            'Rhode Island Actual Load (MW)'                     : 'Rhode Island',
            'Vermont Actual Load (MW)'                          : 'Vermont',
            'Local Timestamp Eastern Time (Interval Beginning)' : 'Local Start Time'
            }
        )
        .with_columns(
            pl.col('Local Start Time')
                .str.to_datetime('%m/%d/%Y %H:%M')
                .dt.replace_time_zone('US/Eastern',
                #  day light saving time, where hour changes by 1,  creates an
                # ambiguity error. ambigous parameter takes care of it 
                ambiguous='latest'
            )
        )
        .with_columns(  # merge 3 regions of Massachusetts for statewide data
            Massachusetts = (
                pl.col('Northeast Massachusetts Actual Load (MW)') +
                pl.col('Southeast Massachusetts Actual Load (MW)') +
                pl.col('Western/Central Massachusetts Actual Load (MW)')
            )
        )
        .with_columns(DATE=pl.col('Local Start Time').dt.date())
        .with_columns(WEEK_NUM = pl.col('Local Start Time').dt.week())
        .with_columns(DAY = pl.col('Local Start Time').dt.strftime('%a'))
        .with_columns(
            DAY_NUM = pl.col('Local Start Time')
            .dt.strftime('%w')
            .cast(pl.Int8)
            )
        .with_columns(
            HOUR = pl.col('Local Start Time')
            .dt.strftime('%H')
            .cast(pl.Int8)
            )
        .select(
            ['Local Start Time', 'DATE', 'WEEK_NUM', 'DAY', 'DAY_NUM', 'HOUR'] 
            + new_england_states
        )
        .sort('DATE')
        .collect() # returns a polars dataframe from a Lazyframe
    )
df = tweak()

#-------------------------------------------------------------------------------
#   Normalize all data, by dividing it by the state population
#-------------------------------------------------------------------------------
for state in new_england_states:
    df = (
        df
        .with_columns(  # divice all values by the population
            pl.col(state)
            /
            df_pop
            .filter(pl.col('State') == state)
            ['POP']
            [0]
        )
        .with_columns(  # multiply by 1000, changes MW to KW
            pl.col(state)*1000
        )
    )

#-------------------------------------------------------------------------------
#   Aggregate by Date, and plot
#-------------------------------------------------------------------------------
df_by_date = (
    df
    .group_by('DATE')
    .agg(pl.col(pl.Float64).sum())
    .sort('DATE')
)
fig = get_fig(df_by_date, 'DATE', my_custom_data = ['DATE'])
fig.add_vrect(
    x0='2024-06-20', 
    x1='2024-09-22',
    fillcolor='green',
    opacity=0.1,
    line_width=1,
)
fig.add_annotation(
    x=0.75, xref= 'paper',  
    y=1,   yref='paper',  
    showarrow=False,
    text='<b>Summer</b>',
)
fig.show()

#-------------------------------------------------------------------------------
#   Aggregate & plot by DAY. df_day_map maps created to control sort order  
#-------------------------------------------------------------------------------
df_day_map = (
    pl.DataFrame(
        {
            'DAY_NUM'   : list(range(7)),
            'DAY'       : ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
        }
        )
    .with_columns(pl.col('DAY_NUM').cast(pl.Int8))
)
df_by_day = (
    df
    .group_by('DAY_NUM')
    .agg(pl.col(pl.Float64).sum()/52)
    .sort('DAY_NUM')
    .join(
        df_day_map,
        on='DAY_NUM',
        how='left'
    )
)
fig = get_fig(df_by_day, 'DAY')
fig.add_vrect(
    x0=1, 
    x1=5,
    fillcolor='green',
    opacity=0.1,
    line_width=1,
)
fig.add_annotation(
    x=0.5, xref='paper',    
    y=0.1,  yref='paper',  
    showarrow=False,
    text='<b>Business Days</b>',
)
fig.show()

#-------------------------------------------------------------------------------
#   Aggregate by Hour Number
#-------------------------------------------------------------------------------
df_by_hour = (
    df
    .group_by('HOUR')
    .agg(pl.col(pl.Float64).mean())
    .sort('HOUR')
)
fig = get_fig(df_by_hour, 'HOUR')
fig.add_vline(
    x=12, 
    line_width=1,
)
fig.add_annotation(
    x=11, xref='x',    
    y=1,  yref='paper',  
    showarrow=False,
    text='A.M.',
)
fig.add_annotation(
    x=13, xref='x',    
    y=1,  yref='paper',  
    showarrow=False,
    text='P.M.',
)
fig.show()

#-------------------------------------------------------------------------------
#   Aggregate by Week Number, and plot
#-------------------------------------------------------------------------------
df_by_week = (
    df
    .group_by('WEEK_NUM')
    .agg(pl.col(pl.Float64).sum())
    .sort('WEEK_NUM')
)
summer_start = 25  # June 20 is in work_week 25
summer_end =  38   # Sept 22 is in work_week 38

fig = get_fig(df_by_week, 'WEEK_NUM')
fig.add_vrect(
    x0=summer_start, 
    x1=summer_end,
    fillcolor='green',
    opacity=0.1,
    line_width=1,
)
fig.add_annotation(
    x=0.7,  xref='paper',   
    y=0.2,  yref='paper',  
    showarrow=False,
    text='<b>Summer</b>',
)
fig.show()
