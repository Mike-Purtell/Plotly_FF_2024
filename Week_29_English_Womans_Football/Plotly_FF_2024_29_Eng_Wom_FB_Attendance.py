import polars as pl
import plotly.express as px
import plotly.graph_objects as go

my_arrow_size = 1
my_arrow_style = 1
my_arrow_width = 1.5

def custom_annotation(fig, text, showarrow, x, y,  xanchor, yanchor, xshift, yshift, align, ax=0, ay=0):
    fig.add_annotation(
        text = text,
        showarrow=showarrow,
        arrowwidth=my_arrow_width,
        arrowsize= my_arrow_size,
        arrowhead = my_arrow_style,
        x = x,
        y = y,
        # xref = 'paper', 
        # yref = 'paper',
        xanchor=xanchor,
        yanchor=yanchor,
        xshift=xshift,
        yshift=yshift,
        font=dict(size=10, color="grey"),
        align=align,
        ax=ax,
        ay=ay
    )
    return fig

df_appearances = (
    pl.read_csv('ewf_appearances.csv')
    .filter(
        pl.col('attendance') != 'NA',
        pl.col('tier') == 1
    )
    .with_columns(
        pl.col('attendance').str.replace(',', '').cast(pl.Int32),
        SEASON = pl.col('season').str.slice(0,4).cast(pl.Int32)
    )
    .with_columns(
         Q1 = pl.col('attendance').quantile(0.25).over('season').cast(pl.Int32),
         Q3 = pl.col('attendance').quantile(0.75).over('season').cast(pl.Int32),
    )
    .with_columns(
         IQR = (pl.col('Q3') -  pl.col('Q1')).cast(pl.Int32)
    )
    .with_columns(
         OUTLIER_L = (pl.col('Q1') - 1.5 * pl.col('IQR')).cast(pl.Int32),
         OUTLIER_H = (pl.col('Q3') + 1.5 * pl.col('IQR')).cast(pl.Int32),
    )
    .with_columns(
         IS_OUTLIER = (
             pl.when(
                 (pl.col('attendance') > pl.col('OUTLIER_H')) 
                 | 
                 (pl.col('attendance') < pl.col('OUTLIER_L'))
             )
         .then(pl.lit(True))
         .otherwise(pl.lit(False))
        )
    )
    .with_columns(MEDIAN = pl.col('attendance').median().round(0).over('season'))       
    .select(pl.col('SEASON', 'attendance', 'MEDIAN', 'Q1', 'Q3', 'IQR','OUTLIER_L','OUTLIER_H','IS_OUTLIER'))
    .unique(subset='SEASON')
    .sort('SEASON')
)
df_no_fliers = df_appearances.filter(pl.col('IS_OUTLIER') == False)

fig  = px.line(df_no_fliers, 'SEASON', 'MEDIAN')
fig.layout.template = 'plotly_white' 

fig.add_trace(
    go.Scatter(
        # connectgaps=False,
        x=df_no_fliers['SEASON'],
        y=df_no_fliers['MEDIAN'],
        marker=dict(color='blue',  size=10), 
    ),
)

fig.update_layout(
    title=go.layout.Title(
        text="English Woman's Football, Tier 1",
        xref="paper",
        x=0
    ),
    xaxis=go.layout.XAxis(
        title=go.layout.xaxis.Title(
            text='' # "Year refers to start of the season<br>SOURCE: The English Women's Football (EWF) Database, May 2024, https://github.com/probjects/ewf-database.",
            )
    ),
    yaxis=go.layout.YAxis(
        title=go.layout.yaxis.Title(
            text='League-wide median attendance'
            ),
    )
)

fig.update_layout(yaxis_range=[0, 3500])

text = "Year refers to start of the season<br><br>"
text += "SOURCE: The English Women's Football (EWF) Database, May 2024  "
text += '<a href="https://github.com/probjects/ewf-database">https://github.com/probjects/ewf-database</a>'
fig.add_annotation(
    text = text, showarrow=False, x = 0, y = -0.05, xref='paper', yref='paper',
    xanchor='left', yanchor='top', xshift=0, yshift=0,
    font=dict(size=10, color="grey"), align="left",
)

# Annotate 2015
text = f'2015 World Cup, Canada.<br>EWF Attendance up 50%<br>54,000 people attended match<br>when 3rd place England beat Canada<br>'
fig = custom_annotation(fig, text, showarrow=True,  x = 2015, y = 951,  xanchor='center', yanchor='bottom', xshift=-1, yshift=10,  align="left", ax=-50, ay=-20)

# Annotate 2019
text = f'2019 World Cup, France.<br>EWF Attendance up 97% after <br>England finished 4th<br>Season shortened by Covid'
fig = custom_annotation(fig, text, showarrow=True,  x = 2019, y = 1306,  xanchor='center', yanchor='bottom', xshift=0, yshift=10,  align="left", ax=0,ay=-20)

# Annotate 2021
text = f'2020 cancelled,<br>league play resumed in 2021'
fig = custom_annotation(fig, text, showarrow=True,  x = 2021, y = 1439,  xanchor='center', yanchor='top', xshift=0, yshift=-20,  align="left", ay=30)

# Annotate 2022
text = f'2022: attendance up 100%'
fig = custom_annotation(fig, text, showarrow=True,  x = 2022, y = 2966,  xanchor='right', yanchor='middle', xshift=-10, yshift=0,  align="left", ax=-50, ay=25)

# Annotate Summer League
text = f'<b>Summer schedule</b><br>'
fig = custom_annotation(fig, text, showarrow=False,  x = 2012.5, y = 3200,  xanchor='left', yanchor='top', xshift=0, yshift=10,  align="left", ax=0,ay=-20)

# Annotate Summer League
text = f'<b>Fall to Spring schedule</b> <br>Traditional English football season'
fig = custom_annotation(fig, text, showarrow=False,  x = 2017, y = 3200,  xanchor='left', yanchor='top', xshift=0, yshift=10,  align="left", ax=0,ay=-20)

# Annotate Takeaway
text = f'<b>Early years, low attendance</b>'
fig = custom_annotation(fig, text, showarrow=False,  x = 2012, y = 3500,  xanchor='left', yanchor='top', xshift=0, yshift=10,  align="left", ax=0,ay=-20)

# Annotate Takeaway
text = f'<b>Big attendance boost in 2022</b>'
fig = custom_annotation(fig, text, showarrow=False,  x = 2017, y = 3500,  xanchor='left', yanchor='top', xshift=0, yshift=10,  align="left", ax=0,ay=-20)


fig.update_layout(
    autosize=False,
    width=800,
    height=600,
    showlegend=False,
)

fig.add_vrect(
    x0=2012, x1=2016.5,
    y0=0, y1=0.95,
    fillcolor="LightSalmon", opacity=0.25,
    layer="below", line_width=0,
)
fig.add_vrect(
    x0=2016.5, x1=2022.5,
    y0=0, y1=0.95,
    fillcolor="wheat", opacity=0.25,
    layer="below", line_width=0,
)

fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False)
fig.update_layout(margin=dict(t=100))
fig.show()
fig.write_html('EWF-League-Wide-Attendance.html')
