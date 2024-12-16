import pandas as pd
import plotly.express as px
import plotly.io as pio

df = pd.read_csv("https://raw.githubusercontent.com/plotly/Figure-Friday/refs/heads/main/2024/week-48/API_IT.NET.USER.ZS_DS2_en_csv_v2_2160.csv")
df.head()

melted_data = pd.melt(
    df,
    id_vars=['Country Name'],
    var_name='Year',
    value_name='Quantity'
)

melted_data['Year'] = pd.to_numeric(melted_data['Year'], errors='coerce')
melted_data = melted_data.dropna(subset=['Year', 'Quantity'])
print(melted_data)

latest_year = melted_data['Year'].max()
df_last_10_years = melted_data[melted_data['Year'] >= latest_year - 10]

average_quantity = df_last_10_years.groupby('Country Name')['Quantity'].mean().reset_index()

average_quantity_sorted = average_quantity.sort_values('Quantity', ascending=False).head(5)

fig = px.bar(average_quantity_sorted, 
             x='Country Name', 
             y='Quantity', 
             title='Top 5 Countries with the Highest Average Internet Users (as % of population) in the Last Decade',
             labels={'Quantity': 'Average Percentage of Population', 'Country Name': 'Country'},
             color='Quantity')

fig.update_layout(showlegend=False)
fig.update_layout(
    yaxis=dict(
        range=[0, average_quantity_sorted['Quantity'].max() + 0.05],
        tickmode='array',
        tickvals=[i / 100 for i in range(0, int(average_quantity_sorted['Quantity'].max() * 100) + 1, 1)],
        ticktext=[f'{i}%' for i in range(0, int(average_quantity_sorted['Quantity'].max() * 100) + 1, 1)],
        tickformat='%'
    )
)
fig.update_layout(xaxis_tickangle=-45, yaxis=dict(range=[average_quantity_sorted['Quantity'].min() - 0.5, 100]))

fig.show()

average_quantity_sorted = average_quantity.sort_values('Quantity', ascending=True).head(5)

average_quantity_sorted['Quantity'] = average_quantity_sorted['Quantity'] / 100

fig2 = px.bar(average_quantity_sorted, 
              x='Country Name', 
              y='Quantity', 
              title='Bottom 5 Countries with the Lowest Average Internet Users (as % of population) in the Last Decade',
              labels={'Quantity': 'Average Percentage of Population', 'Country Name': 'Country'},
              color='Quantity')

fig2.update_layout(showlegend=False)

fig2.update_layout(
    yaxis=dict(
        range=[0, average_quantity_sorted['Quantity'].max() + 0.05],
        tickmode='array',
        tickvals=[i / 100 for i in range(0, int(average_quantity_sorted['Quantity'].max() * 100) + 1, 1)],
        ticktext=[f'{i}%' for i in range(0, int(average_quantity_sorted['Quantity'].max() * 100) + 1, 1)],
        tickformat='%'
    )
)

fig2.update_layout(xaxis_tickangle=-45)

fig2.show()