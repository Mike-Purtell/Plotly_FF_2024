import polars as pl
import plotly.express as px
import pycountry

#  Functions
def add_annotation(fig, annotation, x, y, align, xanchor, yanchor):
    fig.add_annotation(
        text=annotation,
        xref = 'paper', x=x, yref = 'paper', y=y,
        align= align, xanchor=xanchor, yanchor=yanchor,
        font =  {'size': 12, 'color': 'darkslategray'},
        showarrow=False
    )
    return fig

## NOTES: THIS CSV IS STORED AS A ZIP FILE TO SAVE SPACE AND TO OBEY THE RULES
##        OF GIT FILE SIZES. YOU HAVE TO UNCOMPRESS THE CSV TO RUN THIS SCRIPT
df_csv = pl.read_csv(
    f'./Dataset/survey_results_public.csv',
    ignore_errors=True
)

#  create dictionary with countrys as keys, abbreviations as values
dict_countries = {}
for item in pycountry.countries:
    dict_countries[item.name] = item.alpha_3

#------------------------------------------------------------------------------#
#     AI bias by age group                                                     #
#------------------------------------------------------------------------------#
df_ai_age_pct =  (  # create and process dataframe
    df_csv.lazy()   # polars lazy frames 
    .select('Age', 'AISent')
    .filter(~pl.col('Age').is_in(['Prefer not to say','NA']))
    .with_columns(
        Age = pl.col('Age')
            .str.replace('Under 18 years old', '-17')
            .str.replace(' years old', '')
            .str.replace('65 years or older', '65-'),
    )
    .with_columns(pl.col('AISent').str.replace('Very favorable','Favorable'))
    .with_columns(pl.col('AISent').str.replace('favorable','Favorable'))
    .with_columns(pl.col('AISent').str.replace('favorable','Favorable'))
    .with_row_index()
    .group_by(
        pl.col('Age', 'AISent'))
        .agg(pl.col("index").count())
        .rename({'index': 'Count'})
    .with_columns(
        PCT = (100 * (pl.col('Count')/pl.col('Count').sum().cast(pl.Float64())).over('Age')),
        TOTAL = (pl.col('Count').sum().cast(pl.Float64())).over('Age')
    )
    .collect()  # collect optimizes creation of datframe from lazyframe  
)

#  Make vertical bar chart
fig = px.bar(
    df_ai_age_pct.sort('Age').filter(pl.col('AISent') == 'Favorable'),
    x='Age', 
    y="PCT", 
    color="Age",
    barmode = 'stack',
    custom_data = ['Age', 'PCT', 'Count', 'TOTAL']
    )

#  Setup hover elements
fig.update_traces(
    hovertemplate = '<br>'.join([
    'Ages %{customdata[0]}',
    '%{customdata[1]:.1f}% Favorable',
    '%{customdata[2]:,d} of %{customdata[3]:,d}',
    '<extra></extra>'
    ])
)

#  Clean up the plot, and display it
fig.update_layout(
    title = 'AI Favorabilty, Age Bias',
    height=400, width=800,
    xaxis_title='Age Group',
    yaxis_title='Favorable type response rate (%)',
    yaxis_title_font=dict(size=14),
    xaxis_title_font=dict(size=14),
    margin={"r":50, "t":50, "l":50, "b":50},
    autosize=False,
    showlegend=False,
    template='plotly_white',
)
fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False, range=[25, 60])

fig.update(layout=dict(barcornerradius=10))

#  Add annotations
annotation = "<b>Data Source:</b> 2023 Stack Overflow Annual Developer<br>"
annotation += 'Survey. Favorable type means "Very favorable" or<br>'
annotation += '"Favorable". Age excludes "Prefer not to say" & "NA"<br>'
fig = add_annotation(fig, annotation, 0.5, 1.0, 'left', 'left', 'top')
fig.show()
print('\n\n\n')

#------------------------------------------------------------------------------#
#     Average years of professional coding experience by country
#------------------------------------------------------------------------------#
df_coding_years = (   #  prepare dataframe 
    df_csv
    .select('Country', 'YearsCodePro')
    .filter(~pl.col('YearsCodePro').is_in(['NA']))
    #     Change country names for USA and UK to match values used by pycountry 
    .with_columns(pl.col('Country').str.replace('United States of America', 'United States'))
    .with_columns(pl.col('Country').str.replace('United Kingdom of Great Britain', 'United Kingdom'))
    .with_columns(pl.col('Country').str.replace('United Kingdom and Northern Ireland', 'United Kingdom'))
    .with_columns(pl.col('YearsCodePro').str.replace('Less than 1 year', '0'))
    .with_columns(pl.col('YearsCodePro').str.replace('More than 50 years', '50'))
    .with_columns(pl.col('YearsCodePro').cast(pl.UInt16))
)
#  list countries with 1000 or more survey participants
countries_1k = (
    df_coding_years
    .with_columns(
        Country_Count = pl.col('Country').count().over('Country'),
        )
    .filter(pl.col('Country_Count') > 999)
    .filter(pl.col('Country') != 'Other')
    .unique('Country')
    .sort('Country_Count', descending=True)
    ['Country'].to_list()
)

#  include countries with 1000 or more participants, data between 25th and 75th percentile
df_coding_years = (
    df_coding_years
    .filter(pl.col('Country').is_in(countries_1k))
    .with_columns(
        median_years = pl.col('YearsCodePro').median().over('Country'),
        Q75 = pl.col('YearsCodePro').quantile(0.75).over('Country'),
        Q25 = pl.col('YearsCodePro').quantile(0.25).over('Country')
    )
    .with_columns(country_abbr = (pl.col('Country')).replace(dict_countries))
    .filter(pl.col('YearsCodePro').is_between(pl.col('Q25'),pl.col('Q75')))
    .with_columns(average = (pl.col('YearsCodePro').mean().over('Country')))
    .with_columns(my_text = (pl.col('Country') + pl.lit(': ') + pl.col('average').round(1).cast(pl.String)))
    .sort('median_years')
)
#  Create line plot that shows all companies listed in countries_1k
fig = px.line(
    df_coding_years.sort('average'),
    x='country_abbr',
    y='average',
    custom_data = ['Country', 'average']
    )

fig.update_layout(
    title = '<br>Years Professional Coding Experience<br><sup>Countries with at least 1000 survey responses</sup>',
    height=400, width=800,
    xaxis_title='Country',
    yaxis_title='Years (Average)',
    yaxis_title_font=dict(size=14),
    xaxis_title_font=dict(size=14),
    margin={"r":50, "t":50, "l":50, "b":50},
    autosize=False,
    showlegend=False,
    template='plotly_white',
)

#  Setup hover elements
fig.update_traces(
    mode='markers+lines',
    hovertemplate = '<br>'.join([
    '%{customdata[0]}',
    '%{customdata[1]:.2f} Years',
    '<extra></extra>'
    ])
)
#  add scatter plot to emphasize specific countries.
df_focus = df_coding_years.filter(pl.col('country_abbr').is_in(['IND','CAN','USA','AUS']))
fig = fig.add_traces(
    px.scatter(
        df_focus,
        x='country_abbr', 
        y='average',
        text=df_focus['my_text']
        ).data
)
fig.update_traces(textposition='bottom right')

fig.update_traces(line_color='lightgray', line_width=2, marker_size=10)
 # extend x-axis to include all points and annotations
fig.update_xaxes(range=[-1, len(countries_1k)+2.5]) 
fig.update_xaxes(showgrid=True) 
fig.update_yaxes(showgrid=False)

#------------------------------------------------------------------------------#
#      Annotate                                                                #
#------------------------------------------------------------------------------#
annotation = "<b>Data Source:</b> 2023 Stack Overflow Annual Developer<br>"
annotation += 'Survey. Average includes values between<br>'
annotation += 'quantiles 25 & 75. Arbitrary emphasis on<br>'
annotation += 'min & max averages, and North American countries. '
fig = add_annotation(fig, annotation, 0.2, 0.4, 'left', 'left', 'top')
fig.show()
