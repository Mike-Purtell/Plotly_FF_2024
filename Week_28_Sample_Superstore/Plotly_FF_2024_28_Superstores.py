import polars as pl
import plotly.express as px
import plotly.graph_objects as go

# enumeration list generated in exploratory mode with value counts

enum_category = pl.Enum(['Furniture', 'Office Supplies', 'Technology'])
enum_country_region = pl.Enum(['Canada', 'United States'])
enum_sub_category = pl.Enum(
    ['Accessories', 'Appliances', 'Art', 'Binders', 'Bookcases', 
     'Chairs', 'Copiers', 'Envelopes', 'Fasteners', 'Furnishings', 
     'Labels', 'Machines', 'Paper', 'Phones', 'Storage', 'Supplies', 'Tables']
)
enum_region = pl.Enum(['Central', 'East', 'South', 'West'])

enum_segment = pl.Enum(['Consumer', 'Corporate', 'Home Office'])
enum_ship_mode =  pl.Enum(['First Class', 'Same Day', 'Second Class', 'Standard Class'])


df = (
    pl.read_excel('Sample - Superstore.xlsx')
    .with_columns(
        pl.col('Category').cast(enum_category),
        pl.col('Sub-Category').cast(enum_sub_category),
        pl.col('Country/Region').cast(enum_country_region),
        pl.col('Region').cast(enum_region),
        pl.col('Ship Mode').cast(enum_ship_mode),
        pl.col('Segment').cast(enum_segment),
        SHIP_YEAR = pl.col('Ship Date').dt.year(),
        SHIP_MONTH = pl.col('Ship Date').dt.month(),
        
    )
)

df_sales_by_year_month_country = (
    df.group_by(['SHIP_YEAR', 'SHIP_MONTH', 'Country/Region']).agg(pl.col('Sales').sum())
    .pivot(index = ['SHIP_YEAR', 'SHIP_MONTH'],
           on = 'Country/Region')
    .sort('SHIP_YEAR', 'SHIP_MONTH')
)

df = (
    df.group_by(['SHIP_YEAR', 'SHIP_MONTH', 'Country/Region']).agg(pl.col('Profit').sum())
    .pivot(index = ['SHIP_YEAR', 'SHIP_MONTH'],
           on = 'Country/Region')
    .sort('SHIP_YEAR', 'SHIP_MONTH')
    .with_columns(YEAR_MONTH = pl.col('SHIP_YEAR').cast(pl.String) + '_' + pl.col('SHIP_MONTH').cast(pl.String).str.zfill(2))
    .with_columns(dt_YEAR_MONTH = pl.col('SHIP_YEAR').cast(pl.String) + '_' + pl.col('SHIP_MONTH').cast(pl.String).str.zfill(2))
)

canada_red = '#FF0000'
usa_blue ='#0000FF'
fig = px.line(
    df, 
    x="YEAR_MONTH", 
    y=['Canada', 'United States'],
    template="simple_white",
    color_discrete_map={
        "Canada": canada_red,
        "United States": usa_blue
    },
)
fig.update_traces(line=dict(width=1))

usa_mean = df['United States'].mean()
canada_mean = df['Canada'].mean()

fig.update_layout(
    autosize=False,
    width=800,
    height=600,
    font_family='arial',
    font_color='gray',
    title_font_family='arial',
    title_font_color='gray',
    legend_title_font_color='gray',
)
fig.add_hline(y=canada_mean, line_width=2, line_dash='dot', line_color="gray")
fig.add_hline(y=usa_mean, line_width=2, line_dash='dot', line_color="gray")
# fig.annotate('USA', x=1.0, y=usa_mean, )   # line_width=3, line_dash='dot', line_color="gray")
fig.add_annotation(
    dict(
        font=dict(color=usa_blue,size=12),
        x=1.02,
        y=usa_mean,
        showarrow=False,
        text=f'${usa_mean:,.0f} (USA AVG)',
        textangle=0,
        xanchor='left',
        xref="paper",
       )
  )
fig.add_annotation(
    dict(
        font=dict(
            color=canada_red,
            size=12
        ),
        x=1.02,
        y=canada_mean,
        showarrow=False,
        text=f'${canada_mean:,.0f} (CANADA AVG) ',
        textangle=0,
        xanchor='left',
        xref="paper",
        # yref="paper")
       )
  )
fig.update_layout(title='Sales by Country, USA vs Canada',  font = dict(size=24))
fig.update_layout(xaxis_title='')
fig.update_layout(yaxis_title='SALES (US$)', font = dict(size=16))


fig.update_layout(legend_title='Country')

annotate_string = (
    f'<b><span style="color: {usa_blue}">USA</b> population is 8.5x larger than ' +
    f'<b><span style="color: {canada_red}">Canada</b>, sales volume is 37x larger.<br>' +
    f'<b>Call to Action:</b> Improve sales volume in <b><span style="color: {canada_red}">Canada</b> and ' +
    f'figure out why <span style="color: {usa_blue}">USA</b> sales vary so wildly'
)
fig.add_annotation(
    text= annotate_string,
    align='left',
    showarrow=False,
    xref='paper',
    yref='paper',
    x=0.02,
    y=1.04,
    font = dict(size=12)
)

fig.show()
fig.write_html('USA_Canada_Sales.html')