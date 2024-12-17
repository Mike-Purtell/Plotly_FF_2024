import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import numpy as np

#------------------------------------------------------------------------------#
#     Election winners cannot be determined from the weekly dataset. Two       # 
#     elections were won by candidates who loser of the popular vote.          #
#------------------------------------------------------------------------------#
df_election_winners = (
    # this df created as Lazy Frame for later join with other lazy frame
    pl.LazyFrame(
    {
        'YEAR'    : [1976, 1980, 1984, 1988, 1992, 1996, 2000, 2004, 2008 ,2012, 2016, 2020],
        'WINNER'  : ['Democrat','Republican','Republican','Republican', 'Democrat', 
                     'Democrat','Republican','Republican','Democrat','Democrat',
                     'Republican','Democrat'] 
    }
    )
    .with_columns(pl.col('YEAR').cast(pl.Int16))
)
print(f'{df_election_winners =  }')
# print(f'{df_election_winners.schema =  }')
print(f'{df_election_winners.collect_schema() =  }')
print(f'{df_election_winners.describe =  }')
print(f'{df_election_winners.inspect =  }')

#------------------------------------------------------------------------------#
#     Read dataset from local file, clean up - uses polars LazyFrame           #
#------------------------------------------------------------------------------#
df = (
    pl.scan_csv(       # pl.scan_csv creates Lazy Frame for query optimization
    './Dataset/1976-2020-president.csv',
    ignore_errors=True
    )
    .select(pl.col('year', 'state', 'party_simplified', 'candidatevotes'))
    .rename(
        {
            'year': 'YEAR',
            'state':'STATE',
            'party_simplified':'PARTY',
            'candidatevotes':'VOTES'
        }
    )
    .filter(pl.col('PARTY').is_in(['DEMOCRAT', 'REPUBLICAN']))
    .with_columns(
        pl.col('PARTY')
        .str.replace('DEMOCRAT', 'DEM')
        .str.replace('REPUBLICAN', 'REP'),
        pl.col('YEAR').cast(pl.Int16),
        pl.col('VOTES').cast(pl.Int32)
    )
     # optimize & run query from scan_csv to .collect() on the next line
    .collect()   
    .pivot(
         index =['YEAR', 'STATE'],
         on = 'PARTY',
         values='VOTES',
         aggregate_function='sum'
    )
    # convert back to lazy mode post-pivot
    .lazy()     # converting back to Lazy Frames
    .with_columns(
        STATE_WINNER = 
            pl.when(pl.col('DEM')> pl.col('REP'))
            .then(pl.lit('Democrat'))
            .otherwise(pl.lit('Republican'))
    )
    .with_columns(
        STATE_LOSER = 
            pl.when(pl.col('DEM')> pl.col('REP'))
            .then(pl.lit('Republican'))
            .otherwise(pl.lit('Democrat'))
    )
    .join(
        df_election_winners,
        on='YEAR',
        how='left'
    )
    .with_columns(
        WIN = pl.when(pl.col('STATE_WINNER') == pl.col('WINNER'))
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
    )
    .with_columns(
        LOSS = pl.when(pl.col('STATE_WINNER') != pl.col('WINNER'))
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
    )

    # optimize & run query from previous .lazy() to .collect() on the next line
    .collect()
)

#------------------------------------------------------------------------------#
#     Make scatter plot to show  each state's winning party by year            #
#------------------------------------------------------------------------------#
states = df.columns[0:-2]
df_state_winners = (
    df
    .pivot(
        index = 'YEAR',
        on = 'STATE',
        values = 'STATE_WINNER'
    )
    .with_columns(pl.col('YEAR').cast(pl.String))
)
print(f'{df_state_winners.head(2) = }')
print(f'{df_state_winners.glimpse() = }')

states = df_state_winners.columns[1:]

fig = px.scatter(df_state_winners, 'YEAR', states)
fig.update_layout(
    title = f'USA Elections: State Winner by YEAR'.upper(),
    height=300, width=1000,
    xaxis_title='ELECTION YEAR'.upper(),
    yaxis_title="Winning party".upper(),
    yaxis_title_font=dict(size=14),
    xaxis_title_font=dict(size=14),
    margin={"r":50, "t":50, "l":50, "b":50},
    autosize=False,
    showlegend=True,
    template='plotly_white'
)
#  Setup hover elements
fig.update_traces(
    mode='markers+lines',
    marker=dict(size=12, line=dict(width=0)),
    )
# fig.update_layout(hovermode='x unified')
fig.update_layout(legend_title='STATE')
fig.update_yaxes(range = ['REP', 'DEM'])
fig.update_yaxes(categoryorder='category descending')

fig.show()

#------------------------------------------------------------------------------#
#     Draw a plotly table with the win/loss results                            #
#------------------------------------------------------------------------------#
df_wins_losses = (
    pl.LazyFrame(   # start as a lazy frame
        df
        .group_by('STATE')
        .agg(
            [
                pl.col('WIN').sum(),
                pl.col('LOSS').sum()
            ]
        )
        .sort('WIN', 'STATE', descending =[True, False])
    )
    .collect()    # convert lazy frame to dataframe
)

fig = go.Figure(
    data=[
        go.Table(
            header=dict(
                values=['STATE', 'ELECTION<br>WINS', 'ELECTION<br>LOSSES'],
                line_color='darkslategray',
                fill_color='royalblue',
                align=['left','center'],
                font=dict(color='white', size=20),
                height=40
                ),
                cells = dict (
                    values=[
                            df_wins_losses['STATE'], 
                            df_wins_losses['WIN'],
                            df_wins_losses['LOSS'],
                        ],
                        line_color='gray',
                        fill_color='white',
                        align=['left','center'],
                        font_size=20,
                        height=40
                ),
        )
    ]
)
fig.update_layout(template = 'plotly_white', width=800, height=800)

fig.show()
