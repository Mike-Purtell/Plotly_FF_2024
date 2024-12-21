import plotly.express as px
import polars as pl

# constants
SOURCE_LOCAL = True # if True, data from csv, if False data from get git-repo
csv_local = 'week_46_data.csv'

csv_git_source = 'https://raw.githubusercontent.com/plotly/Figure-Friday/refs/'
csv_git_source += 'heads/main/2024/week-46/PDO_wine_data_IT_FR.csv'

wine_colors = {
    'Rosé'  : '#E3AFA7',  # Rose colors vary, extracted this from a picture
    'Red'   : '#9B2242',  # matches color named winered
    'White' : '#E7DF99',  # white wine is not white, this is deep charconnay
    }

#------------------------------------------------------------------------------#
#     initialize dataframe df_source from local file or git repo
#------------------------------------------------------------------------------#
if SOURCE_LOCAL:   # read cleand-up data from local directory
    df = pl.read_csv(csv_local)       
else:             # read source data from git_repo, and clean-up
    df = (
        pl.read_csv(csv_git_source)
        .with_columns(
            pl.col('Max_yield_hl')
                .cast(pl.UInt16, strict=False),  # False changes na to null         
            pl.col('Country')
                .str.replace('FR', 'France')
                .str.replace('IT', 'Italy')
        )
        .drop_nulls(subset='Max_yield_hl')
        .select(pl.all().exclude('PDOid', 'Info'))
        .with_columns(
            COLOR_RGB = pl.when(pl.col('COLOR') == 'Rosé')
                           .then(pl.lit('#FF0080'))
        )
    )
    df.write_csv(csv_local)
    df.head()

fig = px.violin(
    df,
    x='Color',
    y='Max_yield_hl',
    facet_col='COUNTRY',
    title = (
        'Maximum permitted wine yield (hectoliters per hectare) in France and Italy'
        '<a href="https://en.wikipedia.org/wiki/Yield_(wine)" ' + 
        'style="color:yellow;"> Wikipedia LINK</a>'
    ),
    color='Color', 
    color_discrete_map=wine_colors,
    template='plotly_dark',
)

fig.update_layout(
    font=dict(size=16), 
    showlegend=False,
    # xaxis_title=dict(text='Date', font=dict(size=16, color='#FFFFFF')),
    yaxis_title=dict(text=''),
)

# next line changes facet labels from COUNTRY=xyz, to just show xyz
fig.for_each_annotation(lambda a: a.update(text=a.text.replace("COUNTRY=", "")))

# # this syntax is specific to the faceted plot
fig.update_xaxes(title='')

fig.show()
fig.write_html('Wines.html')