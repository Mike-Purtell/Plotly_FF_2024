import polars as pl
import plotly.express as px

#------------------------------------------------------------------------------#
#     Load data set from git or from local drive                               #
#------------------------------------------------------------------------------#
# read data from git. Uncomment df.write to save a copy on local drive
df = pl.read_csv(
    'https://raw.githubusercontent.com/plotly/Figure-Friday/main/2024/week-34/dataset.csv'
)

#------------------------------------------------------------------------------#
#     Find the top 10 of each genre - ranking based on popularity              #
#------------------------------------------------------------------------------#
top_10_by_genre = (
    pl.LazyFrame(
        df
        .select(pl.col('track_genre', 'artists', 'track_name', 'popularity'))
        .with_columns(ARTIST_COUNT = pl.col('artists').count().over('artists'))
        .unique(['track_genre', 'artists'])
        .group_by(['track_genre', 'artists', 'track_name', 'ARTIST_COUNT']).agg(pl.mean('popularity'))
        .sort(['track_genre','popularity'], descending=True)
        .with_columns(GENRE_COUNT = pl.col('track_genre').count().over('track_genre'))
        .with_columns(TRACK_GENRE_RANK = pl.col('track_genre').cum_count().over('track_genre'))
        .filter(pl.col('TRACK_GENRE_RANK') <= 10)
    )
    .collect()
)

#------------------------------------------------------------------------------#
#     find top 10 genres based on popularity of its songs                      #
#------------------------------------------------------------------------------#
top_10_genres = (
    top_10_by_genre
    .group_by(pl.col('track_genre')).agg(pl.mean('popularity'))
    .sort('popularity', descending=True)
    .head(10)
    .select(pl.col('track_genre'))
    .to_series()
    .to_list()
)

#------------------------------------------------------------------------------#
#      Make bar charts of top 10 songs in the top 10 genres                    #
#------------------------------------------------------------------------------#
for genre in top_10_genres:  # iterate through the top 10 genres
    df_plot = (
        pl.LazyFrame(
            top_10_by_genre
            .filter(pl.col('track_genre') == genre)
            .select(pl.col('artists', 'track_name', 'popularity','track_genre', 'ARTIST_COUNT'))
            .with_columns(
                pl.col('artists').str.to_titlecase(),
            )
            .with_columns(
                ARTIST_TOTAL = pl.col('artists').count().over('artists')
            )
            .with_row_index(offset=1)
            .tail(10)
            .with_columns(
                ARTIST_TRACK =  (
                    pl.lit('<b>') +            #  bold font for artist name
                    pl.col('artists') + 
                    pl.lit('</b>') +           #  end bold font, use normal fon for track name
                    pl.lit('     ') +          #  add spaces after artist name to separate from plot
                    pl.lit('<br>') +           #  html line feed puts artist name on first line,track name on second
                    pl.col('track_name')  +  
                    pl.lit('     ')            #  add spaces after track name to separate from plot
                    )
            )
            .sort('popularity')
        )
        .collect()
    )

    #  Make horizontal bar chart
    fig = px.bar(
        df_plot.sort('popularity', descending=False), 
        x='popularity',
        y="ARTIST_TRACK",
        orientation = 'h',
        template='plotly_white',
        height=600,
        width=1000,
        range_x=[80, 100],
        )
    fig.update_layout(title = genre)
    fig.show()

    #Focus on most popular track by artist
df_plot = (
    df
    .select(pl.col('artists', 'track_name', 'popularity','track_genre'))
    .with_columns(
        pl.col('artists').str.to_titlecase(),
    )
    .with_columns(
        ARTIST_TOTAL = pl.col('artists').count().over('artists')
    )
    # .sort(['ARTIST_TOTAL','popularity',  'artists'], descending=[False, True, False])
    
    .with_columns(
        ARTIST_COUNT = pl.col('artists').cum_count().over('artists')
    )
    .filter(pl.col('ARTIST_COUNT') == 1)
    .sort(['ARTIST_TOTAL','track_genre'], descending=[False, False])
    .with_row_index(offset=1)
    .tail(10)
    .with_columns(ARTIST_TRACK =  pl.lit('<b>') + pl.col('artists') + pl.lit('</b>') +  pl.lit('<br>') + pl.col('track_name'))
    # .write_csv('df.csv')
)
df_plot

#  Make horizontal bar chart
fig = px.bar(
    df_plot,
    x='ARTIST_TOTAL', 
    y="ARTIST_TRACK",
    color = 'track_genre',
    orientation = 'h',
    template='plotly_white',
    height=800,
    width=1500,
    range_x=[100, 300]
    # color="Age",
    #barmode = 'horizontal',
    #custom_data = ['Age', 'PCT', 'Count', 'TOTAL']
    )
fig.show()
