import polars as pl
import plotly.express as px

df = (
    pl.read_csv('child-mortality.csv')
    .filter(
        pl.col('Entity')
        .is_in(['India', 'Brazil', 'United States', 'France', 'United Kingdom', 'Sweden'])
    )
    .select(pl.all().exclude('Code'))
    .pivot(
        index= 'Year',
        on='Entity'
    )
    .sort('Year')
    .with_columns(
        pl.all().exclude('Year').rolling_mean(window_size=10),
    )
)
fig = px.scatter(
    df,
    'Year',
    df.columns[1:]
)
my_title = 'Child Mortality Rate, 1751 to 2021<br>'
my_title += '<sup>The Estimated Share of newborns<sup>1</sub> who die before reaching the age of five</sup>'
fig.update_layout(
    title = my_title,
    height=400, width=800,
    xaxis_title='Data source: UN IGME (2023); Gapminder(2015)',
    yaxis_title='',
    xaxis_title_font=dict(size=14),
    margin={"r":50, "t":50, "l":50, "b":50},
    autosize=False,
    showlegend=True,
    template='plotly_white',
    legend=dict(
        title = ''
    )
)

fig.update_traces(
    mode='lines',
    marker=dict(line=dict(width=1)),
    )

fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False)
fig.show()