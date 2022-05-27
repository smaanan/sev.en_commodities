# Sev.en Commodities Project
## Interactive Dashboard
The python code I am about to describe allows to create a dashboard that displays the aggregated volume of each combination of commodity type and period in time. After running the code below, the user should obtain a data frame that changes interactively according to the input values the user chooses.

## Loading the data
The first step is to import the modules containing necessary functions to conduct our analysis, and read the parquet file containing the data set.

```python
import pandas as pd
import numpy as np
import ipywidgets as widgets
import datetime
from ipywidgets import interactive
from IPython.display import display, Javascript
import warnings
warnings.filterwarnings('ignore')

parquet_file = r'/home/maanan/sevencommodities/random_deals.parq'
df = pd.read_parquet(parquet_file, engine='auto')
```

- `pandas` allows the manipulation of data frames;
- `numpy` allows us to modify numbers in certain colums, e.g. to change the sign of certain values;
- `datetime` allows us to manipulate date data;
- `ipywidgets` allows us to create the widgets through which we will choose data to be displayed.

## Preliminary modifications
Now that we have the data ready, we need to convert the columns containing dates into a format that is appropriate for the analysis. This step will turn out to be crucial when we will run our widgets, as it will allow us to select the transactions to be cascaded based on a date the user would choose.

```python
# Keep the date, remove the time

df['executed_date'] = pd.to_datetime(df['executed_date'], format = '%Y-%m-%d').dt.date
df['first_delivery_date'] = pd.to_datetime(df['first_delivery_date'], format = '%Y-%m-%d').dt.date
df['last_delivery_date'] = pd.to_datetime(df['last_delivery_date'], format = '%Y-%m-%d').dt.date
df['last_trading_date'] = pd.to_datetime(df['last_trading_date'], format = '%Y-%m-%d').dt.date
```

## Creating widgets
In this step we will create the widgets that will allow the user to select the result to show:

```python
## Book widget

books = pd.Categorical(df['book'])
books = books.categories

books_dropdown = widgets.Dropdown(
    options=books,
    value=books[0],
    description='Book:',
    disabled=False,
)
```
This widget will allow the user to select which `book` to analyze the data frame for, e.g. if the user selects `Book_1`, it will create a subset the data frame for only the `book` is equal to `Book_1`.

```python
counterparty = pd.Categorical(df['counterparty'])
counterparty = counterparty.categories

counter_dropdown = widgets.Dropdown(
    options=counterparty,
    value=counterparty[0],
    description='Counterparty:',
    disabled=False,
)
```
This widget does the same thing the previous widget does, but for the `counterparty` variable instead of the `book` variable, i.e. it creates a subset of the data frame where the only `counterparty` is that which the user chooses.

```python
date_picker = widgets.DatePicker(
    description='Pick a Date',
    disabled=False,
)
date_picker.add_class("start-date")

script = Javascript("\
                const query = '.start-date > input:first-of-type'; \
                document.querySelector(query).setAttribute('min', '2020-12-01'); \
                document.querySelector(query).setAttribute('max', '2025-01-01'); \
        ")

```

This one is the widget responsible for choosing the date, i.e. the maturity date, every transaction that took place before this date will be cascaded into future periods according to a function we will describe below. The choice of date the user is allowed is constrained to be within the range of dates in the data frame.

## The cascading function
The cascading function is the most important function in the notebook. It allows the cascading of future contracts into next periods, the reasoning behind cascading is described in the paragraph below:

### Cascading
At the end of the `last_trading_day` of a yearly futures, before calculating initial margins, open positions in such futures are replaced by the following equivalent positions: three monthly futures; January, February, and March, and three Quarterly futures April-June (Q2), July-September (Q3), and October-December (Q4), which, summed up, correspond to the delivery period of the yearly future.

Example:

A long yearly futures position for which the `last_trading_date` is 20 December 2007 will be split into multiple positions as follows:

![Cascading_2](https://user-images.githubusercontent.com/22676439/170587156-6f9d0d34-58fb-414e-98a9-d34edceae484.png)

The same logic applies to quarterly futures; at the end of the `last_trading_date` of a quarterly futures, before calculating initial margins, open positions in such futures are replaced by the equivalent positions in the monthly futures whose delivery period is equivalent to the delivery period of the quarterly futures.

Example:

A long quarterly position for which the `last_trading_date` is 26 March 2008 will be split into multiple positions as follows:

![Cascading_4](https://user-images.githubusercontent.com/22676439/170587219-6da0ad03-c58d-4eef-bdce-8b8ea60ee08e.png)

### The functions
Before doing the actual cascading, it is important to change the periods into an appropriate format. For example, some transactions have a `quarter` as `tenor` value, but display a month like `Jan 22` in the `delivery_window` column. The code below fixes this issue and changes the value in `delivery_window` to align with the value in `tenor`. A quarter `tenor` where the `delivery_window` is `Jan 22` will become `Q1 22`.

```python
def ConvtoQuarter(tenor, delivery):
  if tenor == "quarter":
    if delivery[:3] in ['Jan', 'Feb', 'Mar']:
      return "Q1 "+delivery[-2:]
    elif delivery[:3] in ['Apr', 'May', 'Jun']:
      return "Q2 "+delivery[-2:]
    elif delivery[:3] in ['Jul', 'Aug', 'Sep']:
      return "Q3 " + delivery[-2:]
    elif delivery[:3] in ['Oct', 'Nov', 'Dec']:
      return "Q4 " + delivery[-2:]
  else:
      return delivery
```

After the months are converted based on the values in `tenor`, the cascading function `split_tenor` will be used. It applies the cascading process as described above and splits the corresponding `volume` accordingly.

```python
def split_tenor(row):
    start, year = row['new_window'].split(" ")
    if start == "Cal":
        months = ["Jan", "Feb", "Mar", "Q2", "Q3", "Q4"]
        year = int(year) + 1
    elif start == "Q1":
        months = ["Jan", "Feb", "Mar"]
    elif start == "Q2":
        months = ["Apr", "May", "Jun"]
    elif start == "Q3":
        months = ["Jul", "Aug", "Sep"]
    elif start == "Q4":
        months = ["Oct", "Nov", "Dec"]
    else:
        return row['new_window'], row['volume']

    if start == "Cal":
        split_vol = [row['volume']/12] * 3 + [row['volume']/4] * 3
    else:
        split_vol = [row['volume']/len(months)] * len(months)
    
    return [f"{m} {year}" for m in months], split_vol

```

## The widget
After we created the widgets and the functions that will allow us to implement the cascading process, it is time to describe the main function `filter_function` where all the necessary calculations will be made and the output generated. We will show the function below and then describe the role of each line of code inside it.

```python
def filter_function(bookcode, cpartycode, datecode):
                
    filtered = df[(df['book'] == bookcode) & (df['counterparty'] == cpartycode)]
        
    filtered = filtered.drop(['deal_id','book','counterparty','strategy','commodity_code','trading_unit'], axis=1)
            
    filtered.loc[filtered['buy_sell'] == 'sell', 'volume'] = -filtered['volume']
        
    filtered['new_window'] = filtered.apply(lambda x: ConvtoQuarter(x['tenor'], x['delivery_window']), axis=1)
            
    split_1 = filtered[filtered['last_trading_date'] < datecode].copy()
    
    split_2 = filtered[filtered['last_trading_date'] >= datecode].copy()
        
    split_1["new_window"], split_1["new_volume"] = zip(*split_1[["volume", "new_window"]].apply(split_tenor, axis = 1))
            
    split_1 = split_1.explode(["new_window", "new_volume"])
    
    split_2['new_volume'] = split_2['volume']
        
    filtered = pd.concat([split_1, split_2], ignore_index=True)
            
    filtered = pd.pivot_table(filtered, values="new_volume", index=["new_window"], columns=["commodity_name"], aggfunc=np.sum)
    
    with report_output:
        report_output.clear_output()
        display(filtered.round(1))  
        
w = interactive(filter_function, bookcode=books_dropdown, cpartycode=counter_dropdown, datecode=date_picker) 

display(w)
report_output = widgets.Output()
display(report_output)
```

The command
```python
filtered = df[(df['book'] == bookcode) & (df['counterparty'] == cpartycode)]
```
will create a subset `filtered` of the main data frame `df`, where only the chosen `Book` and `Counterparty` will be kept.

```python
filtered = filtered.drop(['deal_id','book','counterparty','strategy','commodity_code','trading_unit'], axis=1)
```
will remove the extra variables that are no longer necessary for our analysis.

```python
filtered.loc[filtered['buy_sell'] == 'sell', 'volume'] = -filtered['volume']
```
will change the sign of the values in `volume` based on the category in `buy_sell`, i.e. when `buy_sell` is `sell`, the `volume` will be negative, and will remain positive when `buy_sell` is `buy`.

```python
filtered['new_window'] = filtered.apply(lambda x: ConvtoQuarter(x['tenor'], x['delivery_window']), axis=1)
```
will create a new variable, `new_window`, that will apply the function `ConvtoQuarter` in order to convert the months into their corresponding quarters.

```python
split_1 = filtered[filtered['last_trading_date'] < datecode].copy()
    
split_2 = filtered[filtered['last_trading_date'] >= datecode].copy()
```
will split the data frame `filtered` into two parts, one part is where `last_trading_date` occured before, i.e. for which the maturity period passed and the futures need to be cascaded, and another part that does not need cascading and for which no changes will occur.

```python
split_1["new_window"], split_1["new_volume"] = zip(*split_1[["volume", "new_window"]].apply(split_tenor, axis = 1))

split_1 = split_1.explode(["new_window", "new_volume"])
    
split_2['new_volume'] = split_2['volume']
```
will call the `split_tenor` function and perform the cascading for `split_1`. `split_2` will remain the same.

```python
filtered = pd.concat([split_1, split_2], ignore_index=True)
```
will rejoin the two data frames.

```python
filtered = pd.pivot_table(filtered, values="new_volume", index=["new_window"], columns=["commodity_name"], aggfunc=np.sum)
```
will create the output table, it aggregates the `volume` for each combination of commodity type and period.

```python
w = interactive(filter_function, bookcode=books_dropdown, cpartycode=counter_dropdown, datecode=date_picker) 
```
will make the function interactive, i.e. every time the user selects a different value for `Book`, `Counterpart` and date, the function updates the displayed result.

### Warning

Whenever I open the notebook and run it for the first time, an error appears, which I could not solve and for which I don't know the cause, however when I run the functions again by choosing different values in the widgets the error disappears. This is strange because when I run the functions outside the widget no error appears.
