These scatter plots for week 43 show the number of repair cases by product age. Similar to the histograms shown in the week 43 announcement, the scatter plots provide a clearer visualization when the distributions have similar median values. All of these distributions peak between year 3 and year 5.

I dropped data where repair_status is unknown (only 1 row out of 75k+)

The x-axis ranges are set to 40 years max. All of the data are retained and can be viewed by hitting the plotly autoscale button.

The first scatter plot has a linear y-axis, the second plot has a logarithmic y-axis. 

This week I learned how to set either axis scale to logarithmic. I tried replacing of count values with their base-10 logarithms, and plotting them. This produces identical traces, however showing log values as axis labels and hover info (3.0 for 1000, etc) is much less intuitive. I learn something new on every figure friday project. 

There is a local peak at year 10. Wondering what this means, maybe warranty or service contract expiration?

The log scale shows the 3 traces with consistent separation. This mean that the ratio between traces is maintained even as the volume of repair counts decreases with product age.

I used the pycountry library to map the 3-letter abbreviations of each country to the full country names. That said, these visualizations do not break out the data by country, but I leave this in the code for future use by me or anyone else.

Here are the screen shots and the code:

