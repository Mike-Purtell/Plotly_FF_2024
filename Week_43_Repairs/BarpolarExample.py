# Import libraries
import plotly.graph_objects as go
import pandas as pd

df = pd.read_csv(
    'OpenRepair_Data_RepairCafeInt_202407.csv',
    low_memory=False
)
# only one records = 'Unknown'
df = df[df['repair_status']!='Unknown']
print(df.head())


def combine_data(df, col_base, col_add):
    count = df[col_base].value_counts()
    count_norm = df[col_base].value_counts(normalize=True)
    # Combine counts and normalized data into a single DataFrame
    comb_df = pd.concat([count, count_norm], axis=1)
    comb_df.columns = ['total', 'percentage']
    # Add rank column
    comb_df['rank'] = comb_df['total'].rank(ascending=False, method='dense').astype(int)
    # Adding columns with normalized values
    ct_norm = pd.crosstab(df[col_base], df[col_add], normalize='all')
    comb_df = comb_df.join(ct_norm).reset_index(names='category')

    return comb_df

comb_df_all = combine_data(df, col_base='product_category', col_add='repair_status').sort_values(by='total')

marker_color_map={'Fixed':'#7fbf7b', 'Repairable':'#2b83ba', 'End of life':'#c2cfda'} 

fig = go.Figure()
for col, mc in marker_color_map.items():
    fig.add_barpolar(
        r=comb_df_all[col], 
        customdata = list(zip(comb_df_all['percentage'], [col] * len(comb_df_all))),
        hovertemplate='Repair rate: %{customdata[0]:,.1%}'+
                        '<br>%{customdata[1]} : %{r:.1%}<extra></extra>', 
        theta=[f'{n}<br> {v:,.0f}' for n, v in comb_df_all[['category', 'total']].values],
        name=col, opacity=0.9, marker_color=mc)

fig.update_layout(
    title='Top 10 Product Categories by Repair Status', title_font_size=20,
    width=600, height=600, margin=dict(l=10), template=None, 
    legend=dict(orientation='h', x=0.1, title='Repair Status'),   
    polar = dict(
        sector=[0, 90],
        radialaxis = dict(showticklabels=True, ticks='',
                            nticks=5, tickformat='.1%',),
        angularaxis = dict(showticklabels=True, ticks='',                            
                            rotation=95))
)        
fig.show()        