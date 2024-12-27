import plotly.express as px
import polars as pl
#-------------------------------------------------------------------------------
#    Read csv data to polars dataframe, clean the data.  
#-------------------------------------------------------------------------------
df_source = (
    pl.read_csv(
        'ors-limited-dataset.csv',
    )
    .filter(pl.col('CATEGORY') == 'Prior work experience')
    .filter(pl.col('ESTIMATE TEXT').str.contains('percentile'))
    # .filter(pl.col('OCCUPATION') != 'All workers')
    .with_columns(
        OCCUPATION = 
        pl.when(pl.col('OCCUPATION').str.starts_with('Executive secretaries'))
          .then(pl.lit('Exec Admin')).otherwise('OCCUPATION')
    )
    .with_columns(pl.col('ESTIMATE').cast(pl.Float32).cast(pl.Int16))
    .with_columns(MED_YEARS_EXP = pl.col('ESTIMATE')/365)
    .with_columns(PERCENTILE =  pl.lit(0))  # initialize all values to zero
    .with_columns(
        PERCENTILE = 
            pl.when(pl.col('ESTIMATE TEXT').str.contains('10'))
                .then(pl.lit(10))
                .when(pl.col('ESTIMATE TEXT').str.contains('25'))
                .then(pl.lit(25))
                .when(pl.col('ESTIMATE TEXT').str.contains('50'))
                .then(pl.lit(50))
                .when(pl.col('ESTIMATE TEXT').str.contains('75'))
                .then(pl.lit(75))
                .when(pl.col('ESTIMATE TEXT').str.contains('90'))
                .then(pl.lit(90))                                                
    )
    .pivot(
        on = 'OCCUPATION',
        index='PERCENTILE',
        values= 'MED_YEARS_EXP'
    )
    .sort('PERCENTILE')
)

#-------------------------------------------------------------------------------
#    Find columns that have data for all 5 percentiles, use for scatter plot
#-------------------------------------------------------------------------------
cols_w_all_data = sorted(
    [
        c for c in df_source.columns 
        if (len(df_source[c].drop_nulls()) == 5) 
    ]
)
fig=px.scatter(
    df_source,
    'PERCENTILE',
    cols_w_all_data,
    template='simple_white',
    title='Experience by Occupation'.upper(),
    width=1200, height=500
)
fig.update_layout(
    xaxis_title='Percentile'.upper(),
    yaxis_title='Required Years of Experience'.upper(),
    xaxis_showgrid=True,
    legend={'title':'Occupation'}
    )
fig.update_traces(mode='markers+lines')
fig.update_xaxes(
    tickmode = 'array',
    tickvals = [10, 25, 50, 75, 90],
    ticktext= [10, 25, 50, 75, 90],
    )

fig.show()

#-------------------------------------------------------------------------------
#    Make a clean plot showing 3 selected occupations, and All workrs
#-------------------------------------------------------------------------------
color_dict ={
    'All workers': px.colors.qualitative.G10[0], # px.colors.qualitative'red',
    'Bartenders':px.colors.qualitative.G10[1], #'green',
    'Exec Admin':px.colors.qualitative.G10[2], #'blue',
    'Financial managers':px.colors.qualitative.G10[3], #'yellow
}
plot_cols = sorted(color_dict.keys())
plot_colors = [color_dict.get(c) for c in plot_cols]

fig=px.scatter(
    df_source,
    'PERCENTILE',
    plot_cols,
    template='simple_white',
    title='Experience by Occupation'.upper(),
    width=800, height=500
)
fig.update_layout(
    xaxis_title='Percentile'.upper(),
    yaxis_title='Required Years of Experience'.upper(),
    xaxis_showgrid=True,
    showlegend=False
    )
fig.update_traces(mode='markers+lines')
fig.update_xaxes(
    tickmode = 'array',
    tickvals = [10, 25, 50, 75, 90],
    ticktext= [10, 25, 50, 75, 90],
    )
#-------------------------------------------------------------------------------
#    Touch up and annotate
#-------------------------------------------------------------------------------
annotation_x_offset = 1
for i in range(4):
    fig['data'][i]['line']['color']=plot_colors[i]
    fig['data'][i]['marker']['color']=plot_colors[i]
    x_annotate = annotation_x_offset + df_source['PERCENTILE'].to_list()[-1]
    y_annotate = df_source[plot_cols[i]].to_list()[-1]
    fig.add_annotation(
        x=x_annotate, xanchor='left',
        y=y_annotate, 
        showarrow=False,
        text=f'{plot_cols[i]}',
        align='right',
        font=dict(size=14, color=plot_colors[i]),
    )
fig.show()