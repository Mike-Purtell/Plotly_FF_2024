import polars as pl
import plotly.express as px
import pycountry
pl.show_versions()

#------------------------------------------------------------------------------#
#  MAP COUNTRY ABBREVIATIONS TO FULL NAMES, USING PYCOUNTRY LIBRARY            #
#------------------------------------------------------------------------------#
df_countries = (
    pl.DataFrame(
        dict(
            zip(
                [c.name for c in pycountry.countries],
                [c.alpha_3 for c in pycountry.countries]
            )
        )
    )
    .transpose(include_header=True)
    .rename({'column': 'COUNTRY', 'column_0': 'CTRY_ABBR'})
)

#------------------------------------------------------------------------------#
#  READ DATA SET, TWEAK AND CLEAN FOR THIS EXERECISE                           #
#------------------------------------------------------------------------------#
df = (
    pl.read_csv('OpenRepair_Data_RepairCafeInt_202407.csv')
    .rename({'country': 'CTRY_ABBR'})
    .join(
        df_countries,
        on='CTRY_ABBR',
        how='left'
    )
    .with_columns(pl.col('product_age').cast(pl.UInt16))
    .with_columns(
        PRODUCT_AGE_COUNT = 
            pl.col('repair_status')
            .count()
            .over(['repair_status','product_age'])
            )
    .drop('problem')  # 66_071 unique problems out or 75252 entries, not useful
    .drop('group_identifier')  # too inconsistent, not useful    
    .drop('product_category_id')  # redundant, used named product category   
    .drop('partner_product_category')      # inconsistent data,      
    .drop('id')      # unique record id for this analysis not needed
    .drop('data_provider')  # all values are Repair Café International
    # only 1 entry for unknown, drop it
    .filter(~pl.col('repair_status').is_in(['Unknown']))              
)
# shift country name and abbr to left side of dataframe, drop first col
left_cols = ['COUNTRY', 'CTRY_ABBR']
reordered_cols = left_cols + [c for c in df.columns[1:] if c not in left_cols]
df = df[reordered_cols]

#------------------------------------------------------------------------------#
#  PREPARE DATAFRAME FOR SCATTER PLOTS                                         #
#------------------------------------------------------------------------------#
df_scatter = (
    df
    .select(pl.col('repair_status','product_age', 'PRODUCT_AGE_COUNT'))
    .unique(['repair_status', 'product_age'])
    .pivot(
         on='repair_status',
         values='PRODUCT_AGE_COUNT',
    )
    .sort('product_age', descending=False)
)

#------------------------------------------------------------------------------#
#  SCATTER PLOT REPAIR COUNT BY PRODUCT AGE, LINEAR SCALE                      #
#------------------------------------------------------------------------------#
plot_cols = ['Fixed',  'End of life', 'Repairable'] 
fig = px.scatter(
    data_frame= df_scatter,
    x = 'product_age',
    y = plot_cols,
    template='simple_white',
    width=800,
    height=500,
)
fig.update_layout(
        title='Linear Scale (Y) of Repair Counts by product age'.upper(),
        xaxis_title='product age [years]'.upper(),
        yaxis_title='linear scale - Repair Count'.upper(),  
        yaxis_range = [0.0, 1400.0],
        xaxis_range=[0,40],
        legend_title=None,
        hovermode='x unified', 
)
fig.update_traces(mode='lines+markers')
fig.show()

#------------------------------------------------------------------------------#
#  SCATTER PLOT REPAIR COUNT BY PRODUCT AGE, LOG SCALE                         #
#------------------------------------------------------------------------------#
fig = px.scatter(
    df_scatter,
    'product_age',
    plot_cols,
    template='simple_white',
    width=800,
    height=500,
    log_y=True,
)
fig.update_layout(
        title='Log Scale (Y) of Repair Counts by product age'.upper(),
        xaxis_title='product age [years]'.upper(),
        yaxis_title='log scale Repair Count'.upper(),  
        yaxis_range = [0.0, 3.5],
        xaxis_range=[0,40],
        legend_title=None,
        hovermode='x unified', 
)
fig.update_traces(mode='lines+markers')
fig.show()
