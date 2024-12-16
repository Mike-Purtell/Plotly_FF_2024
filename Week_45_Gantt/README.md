# Fig_Fri_Week_45_2024
Plotly's weekly data visualization initiative 

This week's challenge uses px.timeline to implement a gantt chart.

The source data is a list of 950 mines in Canada.

For this gantt chart, the selected mines ...

... have produced coal as all or some of its output

... are no longer operating

... were in service for 25 years or longer

The mines are grouped by the province in which they reside. Considerable
amount of polars dataframe manipulation was used for sorting and to insert 
a group row above the mines from each province.

I use gantt charts like this for project management and status reports. Typically
unfinished tasks are color coded to show percent completion.

This chart saved as html file and can be easily share with other team members.  This 
could be a useful component in a dashboard with schedules, yields, shipping levels, etc.

Appreciate any comments or suggestions. If you run this code and get stuck, please reach out to me.
