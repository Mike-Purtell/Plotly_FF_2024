import polars as pl
import polars.selectors as cs
import plotly.express as px
import numpy as np

color_men = '#1FC3AA'
color_women = '#8624F5'
gender_color_dict = {'male' : color_men, 'female': color_women}


df = pl.read_csv('gpg.csv', ignore_errors = True)

def add_annotation(fig, annotation, x, y, align, xanchor, yanchor, xref='paper', yref='paper', xshift=0):
    ''' Generic function to place text on plotly figures '''
    fig.add_annotation(
        text=annotation,
        xref = xref, x=x, yref = yref, y=y,
        align= align, xanchor=xanchor, yanchor=yanchor,
        font =  {'size': 12, 'color': 'darkslategray'},
        showarrow=False,
        xshift = xshift
    )
    return fig

def tweak_quantiles(df):
    ''' Extract gender percentages of 4 salary quantiles  '''
    return(
        df
        .select(pl.col('pb1Female', 'pb1Male','pb2Female', 'pb2Male','pb3Female', 'pb3Male','pb4Female', 'pb4Male'))
        .unpivot(
            variable_name='Cat',
            value_name='Percent'
        )
        .filter(pl.col('Percent') != 'NULL')
        .with_columns(pl.col('Percent').cast(pl.Float32))
        .with_columns(Gender = pl.col('Cat').str.slice(3))
        .with_columns(
            Enum_Quartile = (
                pl.col('Cat')
                .str.replace('pb1Male', 'Q1')
                .str.replace('pb1Female', 'Q1')
                .str.replace('pb2Male', 'Q2')
                .str.replace('pb2Female', 'Q2')
                .str.replace('pb3Male', 'Q3')
                .str.replace('pb3Female', 'Q3')
                .str.replace('pb4Male', 'Q4')
                .str.replace('pb4Female', 'Q4')
            )
        )
        .group_by('Gender', 'Enum_Quartile').agg(pl.col('Percent').mean())
        .pivot(
            on = 'Gender',
            index='Enum_Quartile'
        )
        .with_columns(Quartile = pl.col('Enum_Quartile').str.slice(1).cast(pl.UInt8))
        .sort('Enum_Quartile', descending=False)
    )

#------------------------------------------------------------------------------#
#     Plot Gender proportion of 4 salary quantiles                             #
#------------------------------------------------------------------------------#
df_quantiles = tweak_quantiles(df)
fig=px.scatter(df_quantiles, x='Enum_Quartile', y=['Male', 'Female'], 
               color_discrete_sequence=[color_men, color_women])
print (df_quantiles)
fig.update_layout(
    title = f'Irish Gender Gap',
    height=600, width=800,
    xaxis_title='Income Quartile: Q1 is lowest, Q4 is highest'.upper(),
    yaxis_title='Avg % of employees per income quantile'.upper(),
    yaxis_title_font=dict(size=14),
    xaxis_title_font=dict(size=14),
    margin={"r":50, "t":50, "l":50, "b":50},
    autosize=False,
    showlegend=False,
    template='plotly_white',
)
#  Setup hover elements
fig.update_traces(
    mode='markers+lines',
    marker=dict(size=12, line=dict(width=0)),
    )

fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False)

annotation = f'<b><span style="color:{color_men}">MEN</b></span>'
fig = add_annotation(
    fig, 
    annotation, 
    df_quantiles['Enum_Quartile'][-1],  # x
    df_quantiles['Male'][-1]-1,                   # y
    'left', 
    'left', 
    'middle',
    xref = 'x', 
    yref = 'y', 
    # xshift=10
    )

annotation = f'<b><span style="color:{color_women}">WOMEN</b></span>'
fig = add_annotation(
    fig, 
    annotation, 
    df_quantiles['Enum_Quartile'][-1],  # x
    df_quantiles['Female'][-1]+1,                   # y
    'left', 
    'left', 
    'middle',
    xref = 'x', 
    yref = 'y', 
    )

annotation = '<b>Data Source:</b> Irish Gender Pay Gap Portal (http://paygap.ie)<br><br>'
annotation += "Average percentages of participating companies<br>"
annotation += "are <b>not weighted</b> by number of responses"
fig = add_annotation(fig, annotation, 0.05, 1.0, 'left', 'left', 'top')

w = f'<b><span style="color:{color_women}">women</b></span>'
m = f'<b><span style="color:{color_men}">men</b></span>'
annotation = f'More {w} than {m} in the lowest income quartile,<br>'
annotation += f'50% more {m} than {w} in the highest income quartile<br>'
fig = add_annotation(fig, annotation, 0.4, 0.5, 'left', 'left', 'middle')

fig.show()
