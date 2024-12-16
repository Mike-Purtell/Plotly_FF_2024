import polars as pl
import plotly.express as px
import plotly.graph_objects as go
#------------------------------------------------------------------------------#
#     Read 2 datasets as polars LazyFrames, join on species_code               #
#------------------------------------------------------------------------------#
df_all = (
    (
        pl.scan_csv('./Data_Set/Jornada_quadrat_species_list.csv')
        .collect()
    ).join(
        (
        pl.scan_csv('./Data_Set/Jornada_quadrat_annual_plant_counts.csv')
        .collect()
        ),
        on='species_code'
    )
)

#------------------------------------------------------------------------------#
#     collect each lazyframes, join on the species code                        #
#     add columns for decade, decade based genus count and percentage          #
#------------------------------------------------------------------------------#
df_by_decade = (
    df_all
    .with_columns(
        decade = (pl.col('year')//10).cast(pl.String) + pl.lit('0s')
    )
    .group_by('decade', 'genus', 'species_code', 'species','common_name')
    .agg(pl.col('count').sum())
    .with_columns(GENUS_COUNT = pl.col('count').sum().over('genus'))
    .with_columns(GENUS_PCT = (pl.col('count') / pl.col('GENUS_COUNT')))
    .sort('decade', 'count')
    .pivot(
        on='genus',
        index='decade',
        values='GENUS_PCT',
        aggregate_function="sum"
    )
)
#------------------------------------------------------------------------------#
#     make px.line plot of of genus percentages by decade                      #
#------------------------------------------------------------------------------#
plot_cols = df_by_decade.columns[1:]
fig = px.line(
    df_by_decade,
    'decade',
    plot_cols,
)

#------------------------------------------------------------------------------#
#     update/format to plot                                                    #
#------------------------------------------------------------------------------#
my_title = 'New Mexico Relative Genus Percentage by Decade<br>'
my_title += '<sup>Each Genus totals 100%</sub>'
fig.update_layout(
    autosize=False,
    width=800,
    height=600,
    showlegend=True,
    legend_title="Genus",
    
    title=go.layout.Title(
        text=my_title.upper(),
        xref="paper",
        x=0
    ),
    xaxis=go.layout.XAxis(
        title=go.layout.xaxis.Title(
            text='decade'.upper()
        )
    ),
    yaxis=go.layout.YAxis(
        title=go.layout.yaxis.Title(
            text='Percentage - each genus total is 100%)'.upper()
            ),
    )
)
fig.layout.template = 'plotly_white' 
fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False,tickformat = '.0%')

#------------------------------------------------------------------------------#
#     Add annotation to tell the story                                         #
#------------------------------------------------------------------------------#

text = 'Story of the plot TBD'
text += ''

fig.add_annotation(
    text = text,
    x = 0.05,
    y = 0.9,
    xanchor='left',
    yanchor='top',
    font=dict(size=12, color='blue'),
    align='left',
    xref='paper',
    yref='paper'
)

fig.show()
