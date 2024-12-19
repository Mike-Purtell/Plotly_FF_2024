import polars as pl
import plotly.express as px

def make_histogram(df, my_title='No Title Provided'):
    ''' quick histogram for debug'''
    fig = px.histogram(
        df,
        df.columns[1:],
        template='plotly_white',
        height=400, 
        width=600,
    )
    fig.update_layout(showlegend=False,title=my_title)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    fig.show()
    return

def make_heatmap(
        df, 
        my_max=10000, 
        my_title='No Title Provided', 
        x_title = 'No X title provided',
        y_title = 'No Y title provided',
        hover_entity='No Hover Entity Provided'
        ):
    '''  make the heat map, various data'''
    Y = list(df['from_country'])
    X = list(df.columns)
    fig = px.imshow(
        df,
        x=X,
        y=Y,
        text_auto=True, 
        height=1200, 
        width=1200,
        range_color=(0,my_max),
        labels=dict(x='To ', y='From', color=hover_entity),
    )
    fig.update_layout(
        template='plotly_white',
        title=my_title.upper(), 
        title_font = {"size": 28},
    )

    fig.update_xaxes(title_text = x_title, title_font = {"size": 20})
    fig.update_yaxes(title_text = y_title, title_font = {"size": 20})
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    fig.show()
    return

#------------------------------------------------------------------------------#
#     Load data, each as a Lazy Frame and Data Frame                           #
#------------------------------------------------------------------------------#
df_countries_lazy = pl.scan_csv('./countries.csv')
df_countries = df_countries_lazy.collect()
df_votes_lazy = pl.scan_csv('./votes.csv')
df_votes = df_votes_lazy.collect()

#------------------------------------------------------------------------------#
#     make dataframe for heat maps, without data normalizaitons                #
#------------------------------------------------------------------------------#
df_heat_map = (
    df_votes_lazy
    .select(pl.col('year', 'from_country', 'to_country', 'total_points'))
    .join(
        df_countries_lazy.rename({'country': 'from_country'}),  
        how='left',
        on='from_country'
        )
    .drop('from_country')
    .rename({'country_name':'from_country'})
    .join(
        df_countries_lazy.rename({'country': 'to_country'}),  
        how='left',
        on='to_country'
        )
    .drop('to_country')
    .rename({'country_name':'to_country'})
    .group_by('from_country', 'to_country')
    .agg(pl.col('total_points').sum())
    .with_columns(  # shorten full names of these countries, to uncrowd the axis labels
        pl.col('to_country', 'from_country')
        .str.replace('Serbia and Montenegro', 'Serb & Mont')
        .str.replace('Bosnia & Herzegovina', 'Bos & Herz')
        .str.replace('North Macedonia', 'N. Maced')
        .str.replace('United Kingdom', 'U.K.')
        )
    .collect()   # Lazyframes can't pivot. Collect here to convert to data frame
    .pivot(
        on='to_country',
        index='from_country'
    )
    .sort('from_country')
)

# sort columns alphabetically, with 'from_country on the far left
df_columns = sorted(df_heat_map.columns)
df_heat_map = (
    df_heat_map.select(
        ['from_country'] + 
        [c for c in df_columns if c != 'from_country']
        )
)

#------------------------------------------------------------------------------#
#     Make a histogram of raw data to guide color_range selection              #
#------------------------------------------------------------------------------#

# From this histogram, 300 is a reasonable value for filtering outliers
make_histogram(df_heat_map, my_title='Raw Data')

make_heatmap(
    df_heat_map, 
    my_max=300, 
    my_title=('Eurovision Votes since 1956'.upper()),  
    x_title = 'VOTES TO COUNTRY',
    y_title = 'VOTES FROM COUNTRY',
    hover_entity='Votes'
)

#------------------------------------------------------------------------------#
#     Normalize Dataframe by dividing voteds recieved from any country by      #
#     the giving countrys years or participation                               #
#------------------------------------------------------------------------------#

df_country_participation_years = (
    df_votes
    .select(pl.col('year','from_country'))
    .unique(pl.col('year', 'from_country'))
    .with_columns(COUNTRY_YEAR_COUNT=pl.col('year').count().over('from_country'))
    .join(
        df_countries.rename({'country': 'from_country'}),  
        how='left',
        on='from_country'
    )
    .select(pl.col('country_name','COUNTRY_YEAR_COUNT'))
    .rename({'country_name':'from_country'})
    .with_columns(  # shorten full names of these countries, to uncrowd the axis labels
        pl.col('from_country')
        .str.replace('Serbia and Montenegro', 'Serb & Mont')
        .str.replace('Bosnia & Herzegovina', 'Bos & Herz')
        .str.replace('North Macedonia', 'N. Maced')
        .str.replace('United Kingdom', 'U.K.')
        )
    .unique('from_country')
    .sort('from_country', descending=False)
)

#------------------------------------------------------------------------------#
#     Join years per country with the heatmap dataframe                        #
#------------------------------------------------------------------------------#
df_heat_map = (
    df_heat_map
    .join(
        df_country_participation_years,
        how='left',
        on='from_country'
    )
)

# alphabetic col sort, with 'from_country', 'COUNTRY_YEAR_COUNT' on the left
df_heat_map_columns = sorted(df_heat_map.columns)
left_cols = ['from_country', 'COUNTRY_YEAR_COUNT']
df_normalized_heat_map = (
    df_heat_map
    .select(left_cols + [c for c in df_heat_map_columns if c not in left_cols])
)

data_cols = df_normalized_heat_map.columns[2:]

for country in data_cols:
    participation_years = (
        df_country_participation_years
        .filter(pl.col('from_country') == country)
        .select(pl.col('COUNTRY_YEAR_COUNT'))
        .to_series().to_list()
    )[0]

    for col in data_cols:
        df_normalized_heat_map = (
            df_normalized_heat_map
            .with_columns(
                pl.when(pl.col('from_country') ==  country)
                .then(100*pl.col(col)/participation_years)
                .otherwise(col)
                .cast(pl.UInt16)
                .alias(col)
            )
        )

df_normalized_heat_map = df_normalized_heat_map.drop('COUNTRY_YEAR_COUNT')

#------------------------------------------------------------------------------#
#     Make a histogram of normalized data to guide color_range selection       #
#------------------------------------------------------------------------------#
make_histogram(df_normalized_heat_map, my_title='Normalized Data')

make_heatmap(
    df_normalized_heat_map, 
    my_max=1000, 
    my_title=('Normalized Eurovision Votes since 1956'.upper()),  
    x_title = 'VOTES TO COUNTRY',
    y_title = 'VOTES FROM COUNTRY',
    hover_entity='Normalized Votes'
)
