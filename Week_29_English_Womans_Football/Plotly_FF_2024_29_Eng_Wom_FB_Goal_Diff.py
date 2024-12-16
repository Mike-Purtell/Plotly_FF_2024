import polars as pl
import plotly
import polars.selectors as cs
import plotly.express as px
import plotly.graph_objects as go

def tweak_df_appearances():
    return(
        pl.read_csv('ewf_appearances.csv')
        .filter(pl.col('tier') == 1)
        .with_columns(                     
            pl.col('attendance')
                .str.replace('NA', '0') # when attendance is NA, replace with string 0
                .str.replace(',', '')   # get rid of thousands commas
                .cast(pl.Int32),
            SEASON = pl.col('season').str.slice(0,4).cast(pl.Int32),
        )
        .rename({'date': 'date_str'})
        .with_columns(
            DATE = pl.col('date_str').str.to_date(format='%m/%d/%Y', strict=True)
        )    
        .sort('SEASON', 'team_name', 'DATE')
        .with_columns(
           MATCH_NUM = pl.cum_count('attendance').over('SEASON','team_name'),
           SEASON_GOAL_DIFF = pl.cum_sum('goal_difference').over('SEASON','team_name'),
        )
        .select(
            pl.col(
                'SEASON', 'MATCH_NUM',  'DATE', 'team_name', 
                'goal_difference', 'SEASON_GOAL_DIFF', 'result', 'win', 'loss', 'draw', 'points'
            )
        )
    )


def custom_annotation(fig, text, showarrow, x, y,  xanchor, yanchor, xshift, yshift, align, ax=0, ay=0, my_color='gray'):
    fig.add_annotation(
        text = text,
        x = x,
        y = y,
        xanchor=xanchor,
        yanchor=yanchor,
        xshift=xshift,
        yshift=yshift,
        font=dict(size=10, color="grey"),
        align=align,
        ax=ax,
        ay=ay,
        font_color=my_color
    )

    return fig

df_appearances = tweak_df_appearances()

df_table = (
    df_appearances
    .group_by('SEASON', 'team_name')
    .agg(
        pl.col('win').sum().alias('W'),
        pl.col('draw').sum().alias('D'),
        pl.col('loss').sum().alias('L'),
        pl.col('points').sum().alias('P'),
        pl.col('goal_difference').sum().alias('GD'),
    )
    .with_columns(
        MATCHES = (pl.col('W') + pl.col('D') + pl.col('L'))
    )
    .with_columns(
        PTS_PER_MATCH = (pl.col('P')/pl.col('MATCHES'))
    )
    .sort('SEASON', 'PTS_PER_MATCH', 'GD', descending = [False, True, True])
    .with_columns(RANK = pl.col('SEASON').cum_count().over('SEASON'))
    .select(pl.col(['SEASON', 'RANK','team_name', 'W', 'D', 'L', 'P', 'GD', 'MATCHES', 'PTS_PER_MATCH']))
)
print(df_table)

seasons =  df_appearances['SEASON'].unique().to_list()

df_pivot = (
    df_appearances
    .with_columns(
        SEASON_GOAL_DIFF = pl.cum_sum('goal_difference').over('SEASON', 'team_name')
    )
    .pivot(
        on = 'team_name',
        index=['SEASON', 'MATCH_NUM'],
        values = 'SEASON_GOAL_DIFF'
    )
)
for season in df_pivot['SEASON'].unique().to_list():
    print(season)
    df = (
        df_pivot.filter(pl.col('SEASON') == season)
    )
    df_cols = list(df.columns)
    null_cols = [c for c in df_cols if len(df.filter(pl.col(c).is_not_null())) == 0]
    df = df.drop(null_cols)
    df = (
        pl.concat([df, df.select(pl.all().sum())])
        .with_row_index('ROW_NUM')
    )
    max_row_num = df['ROW_NUM'].max()
    for c in df.columns[2:]:    
        df = (
            df
            .with_columns(
                pl.when(pl.col('ROW_NUM') == max_row_num)
                .then(pl.lit(0))
                .otherwise(c)
                .name.keep()
            )
            .sort('MATCH_NUM', descending=False)
        )

    y_cols = df.columns[3:]
    fig = px.line(
        df,
        x = 'MATCH_NUM',
        y = df.columns[3:],
        template='plotly_white',
        line_shape='hvh'
    )

    for i, col in enumerate(y_cols):
        season_rank = (
            df_table
            .filter(
                pl.col('SEASON') == season,
                pl.col('team_name') == col
            )
            .select(pl.col('RANK'))
            .to_series()
        )[0]
        goal_differential = (
            df_table
            .filter(
                pl.col('SEASON') == season,
                pl.col('team_name') == col
            )
            .select(pl.col('GD'))
            .to_series()
        )[0]
        # print(f'{goal_differential = }')
        my_color = px.colors.qualitative.Plotly[i%10]
        annotate_text = col.replace('Women', '').replace('Ladies', '') + f'({season_rank}, Goal Diff. = {goal_differential})'
        y_final = df.filter(pl.col('MATCH_NUM') == max_row_num)[col][0]
        fig = custom_annotation(fig, annotate_text, False, max_row_num+0.2, y_final, 'left',  'middle',  0, 0, 'left', my_color=my_color) 

    x_label = "MATCH NUMBER<br><sup>"
    x_label += "SOURCE: The English Women's Football (EWF) Database, May 2024<br>"
    x_label += '<a href="https://github.com/probjects/ewf-database">https://github.com/probjects/ewf-database</a>'
    x_label += '</sup>'
    fig.update_layout(
        autosize=False,
        width=800,
        height=600,
        showlegend=False,
        title=go.layout.Title(
            text=f"English Woman's Football, Tier 1  {season} Goal Differential",
            xref="paper",
            x=0
        ),
        xaxis=go.layout.XAxis(
            title=go.layout.xaxis.Title(
                text=x_label
            ),
            range=[0, max_row_num+6],
            showgrid=False,
           
        ),
        yaxis=go.layout.YAxis(
            title=go.layout.yaxis.Title(
                text='Season Goal Differential'
                ),
             showgrid=False,
             zeroline=False
        )
    )

    fig.show()   
