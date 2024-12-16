import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import polars as pl
import polars.selectors as cs

import numpy as np

degree = 5  # used for curve fitting

#
#   MAKE DATAFRAMES
#
df_python = (  # df python is used for scatter plot
    pl.scan_csv('merged_snake_data.csv')
    # when Common Name is missing, use value from Binomial column 
    .with_columns(
        pl.when(pl.col('Common Name').is_null())
          .then('Binomial')
          .otherwise('Common Name')
          .alias('Common Name')
    )
    .with_columns(pl.col('Common Name').str.to_titlecase())

    .filter(pl.col('Family') == 'Pythonidae')
    .select(pl.col('TBL cm','Weight gr','Common Name'))
    .drop_nulls(subset=['TBL cm','Weight gr'])
    # add columns for hover to show length in feet and inches
    .with_columns(TOTAL_INCHES = (pl.col('TBL cm')*0.39370079))
    .with_columns(FEET = (pl.col('TOTAL_INCHES')/12).cast(pl.UInt8))
    .with_columns(INCHES = (pl.col('TOTAL_INCHES') - (12.0*pl.col('FEET'))))
    .with_columns(POUNDS = (pl.col('Weight gr') / 453.59237)) #.cast(pl.UInt8))
    .sort('TBL cm', descending=False)
    .collect()
)

# df_python_longest used for pareto chart showing world's longest pythons
df_python_longest = (
    df_python.sort('TBL cm',descending=True)
    .head(15)
    .select(pl.col('Common Name', 'TBL cm'))
    .sort('TBL cm', descending=False)
)

#
#   SETUP SIDE BY SIDE PLOTS, PARETO ON LEFT, SCATTER ON RIGHT
#
left_title = "Length of the world's longest pythons"
right_title = 'Python BMI Data: Weight vs Height<br>'
right_title += f'<sup>Best-fit polynomial degree of {degree}</sup>'
fig = make_subplots(
    rows=1, cols=2, 
    subplot_titles=(left_title, right_title),
    specs=[
            [
            {'type': 'scatter'}, {'type': 'bar'}
            ]
        ]
    )

#
#   PARETO CHART OF PYTHON LENGTHS IN LEFT PLOT
#
fig.add_trace(
    go.Bar(
        x=df_python_longest['TBL cm'],  
        y=df_python_longest['Common Name'],
        orientation='h',
        hovertemplate = None,
        hoverinfo = 'skip',
    ),
    row=1, col=1,
)
fig.update_xaxes(title_text='Length (cm)', row=1, col=1)

#
#   SCATTER PLOT WITH POLYNOMIAL BEST FIT ON THE RIGHT
#

#   Create best fit data to overlay on scatter
x = df_python['TBL cm']
y = df_python['Weight gr']
# least squares polynomial fit, 
coefs = np.polyfit(x, y, degree)
best_fit_x = np.linspace(min(x), max(x), num=np.size(x))
best_fit_y = np.polyval(coefs, best_fit_x)

fig.add_trace(
    go.Scatter(
        x=df_python['TBL cm'],
        y=df_python['Weight gr'],
        mode='markers',
        marker=dict(color='green'),
        customdata=df_python[['Common Name', 'TBL cm', 'FEET', 'INCHES', 'Weight gr', 'POUNDS']],
        hovertemplate=(
            '<b>%{customdata[0]}</b><br>' +
            'Length: %{customdata[1]} cm ' +
            '(%{customdata[2]} ft, ' +
            '%{customdata[3]:.0f} in)<br>' +
            'Weight: %{customdata[4]:,} gr ' +
            '(%{customdata[5]:.1f} Pounds)<br>' +
            '<extra></extra>'
        ),
    ),
    row=1, col=2
)
fig.update_xaxes(title_text='Length (cm)', row=1, col=2)
fig.update_yaxes(title_text='Weight (grams)', row=1, col=2)

fig.add_trace(
    go.Scatter(
        x=best_fit_x, 
        y=best_fit_y,
        hovertemplate = None,
        hoverinfo = 'skip',
        mode='lines',
        line=dict(color='gray', width=3)
    ),
    row=1, col=2
)

fig.update_layout(
    template='simple_white',
    showlegend=False,
    title=f'<b>Snakes on a Pane</b>',
    title_font={"size": 36}
)

fig.show()
fig.write_html('snakes_on_a_pane.html')
