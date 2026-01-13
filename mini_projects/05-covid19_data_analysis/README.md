
# Problem Statement
Covid-19 data analysis

## Use Cases

## Section 1
* Rename the column infection_case to infection_source
* Select only following columns 
  * 'Province','city',infection_source,'confirmed'
* Change the datatype of confirmed column to integer
* Return the TotalConfirmed and MaxFromOneConfirmedCase for each "province","city" pair
* Sort the output in asc order on the basis of confirmed


## Section 2
* Return the top 2 provinces on the basis of confirmed cases.

## Section 3
* Return the details only for ‘Daegu’ as province name where confirmed cases are more than 10
* Select the columns other than latitude, longitude and case_id
```python
drop_list = ['latitude', 'longitude', 'case_id',]
df = df.select([column for column in df.columns if column not in drop_list])
```
