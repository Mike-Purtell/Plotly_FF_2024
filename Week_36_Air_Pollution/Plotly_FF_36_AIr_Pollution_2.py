import polars as pl
import polars.selectors as cs
import plotly.express as px
import numpy as np

# colors were cloned using MS-Paint Eye Dropper tool
my_color_dict = {   
    '   - 10':  '#A4FFFF',
    '10 - 15':  '#B0DAE9',
    '15 - 20':  '#F9E047',
    '20 - 30':  '#F2C84B',
    '30 - 40':  '#F1A63F',
    '40 - 50':  '#E98725',
    '50 - 60':  '#AF4553',
    '60 - 70':  '#863B47',
    '70 - 80':  '#673A3D',
    '80 - 90':  '#462F30',
    '90 -   ':  '#252424',
}

#------------------------------------------------------------------------------#
#     Load the data                                                            #
#------------------------------------------------------------------------------#

df_pollution = (
    pl.scan_csv('air-pollution.csv')   # Lazy Frame
    .collect() # Run Query, return Dataframe
)

# if enumerating over full list of cities, you will get over 100 plots.
city_list = df_pollution.select(pl.all().exclude('Year')).columns
city_list = ['Beijing, China']
for i, c in enumerate(city_list):
    print(f'City {i+1} of {len(city_list)}')
    df_city = (
        pl.LazyFrame(df_pollution)    # Lazy Frame
        .select(pl.col('Year', c))
        # .with_columns(color_index = (pl.col(c)/5).cast(pl.UInt8))
        .with_columns(BIN = pl.lit('UNDEFINED'))   #initialize new BIN column
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(0, 10, closed='right'))
                    .then(pl.lit('   - 10')).otherwise('BIN')
        )
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(10, 15, closed='right'))
                    .then(pl.lit('10 - 15')).otherwise('BIN')
        )
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(15, 20, closed='right'))
                    .then(pl.lit('15 - 20')).otherwise('BIN')
        )
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(20, 30, closed='right'))
                    .then(pl.lit('20 - 30')).otherwise('BIN')
        )
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(30, 40, closed='right'))
                    .then(pl.lit('30 - 40')).otherwise('BIN')
        )
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(40, 50, closed='right'))
                    .then(pl.lit('40 - 50')).otherwise('BIN')
        )
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(50, 60, closed='right'))
                    .then(pl.lit('50 - 60')).otherwise('BIN')
        )  
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(60, 70, closed='right'))
                    .then(pl.lit('60 - 70')).otherwise('BIN')
        )
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(70, 80, closed='right'))
                    .then(pl.lit('70 - 80')).otherwise('BIN')
        )
        .with_columns(
            BIN = pl.when(pl.col(c).is_between(80, 90, closed='right'))
                    .then(pl.lit('80 - 90')).otherwise('BIN')
        )  
        .with_columns(
            BIN = pl.when(pl.col(c) > 90)
                    .then(pl.lit('90 -   ')).otherwise('BIN')
        )  
        .collect() # Run Query, return Dataframe
    )

    def add_annotation(ig, annotation, align, xanchor, yanchor, x, xref, y, yref,   xshift=0, font_size=14):
        ''' Generic function to place text on plotly figures '''
        fig.add_annotation(
            text=annotation,
            xref = xref, x=x, yref = yref, y=y,
            align= align, xanchor=xanchor, yanchor=yanchor,
            font =  {'size': font_size, 'color': 'darkslategray'},
            showarrow=False,
            xshift = xshift
        )
        return fig

    #------------------------------------------------------------------------------#
    #     setup px.scatter with color stripes, no annotations                      #
    #------------------------------------------------------------------------------#
    fig = px.scatter(
        df_pollution,
        'Year',
        c,
    )
    fig.update_traces(line=dict(color='white',width=6))

    my_title = f'{c}<br>'
    my_title += '<sup>Air pollution (PM2.5) concentrations</sup><br>'
    fig.update_layout(
        template='plotly_white',
        height=600,
        width=900,
        title=my_title,
        title_font=dict(size=24),
        yaxis_title='Annual Mean PM2.5 Concentration'.upper() + ' (μg/m<sup>3</sup>)',
        xaxis_title='',
        yaxis_title_font=dict(size=20),
        yaxis_range=[0,130],
    )

    customdata = np.stack(
        (
            df_pollution['Year'],     
            df_pollution[c]
        ), 
        axis=-1
    )

    hovertemplate = (
        '<b>%{customdata[0]}</b><br>' + 
        'PM2.5 Concentration: %{customdata[1]:,.1f}<br>' + 
        '<extra></extra>')

    fig.update_traces(
        mode='lines',
        customdata=customdata, 
        hovertemplate=hovertemplate,   
        )

    #------------------------------------------------------------------------------#
    #     add a box of width 1 above each year, use color_dict for shading value   #
    #------------------------------------------------------------------------------#
    for year in df_pollution['Year'].to_list():
        my_bin = df_city.filter(pl.col('Year') == year).select(pl.col('BIN')).to_series()[0]
        my_color = my_color_dict.get(my_bin)
        fig.add_vrect(
            x0=year-0.5, x1=year+0.5,
            fillcolor=my_color, #  opacity=0.5,
            layer="below", 
            line_color=my_color,
        )

    fig.data = fig.data[::-1]
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    fig.show()
