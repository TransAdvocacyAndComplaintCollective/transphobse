from owid.catalog import charts
df = charts.get_data('life-expectancy')
print(dir(charts))
xxx = charts.list_charts()
print(xxx)