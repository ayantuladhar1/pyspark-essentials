# City Temperature Analysis
## Problem Statement	
* As a data engineer you are supposed to prepare the data from temp analysis
* Your pipeline should return data in form of following columns
  * city
  * avg_temperature
  * total_temperature
  * num_measurements
* You should return metrics for only those cities when total_temperature is greater than 30
* And output should be sorted on city in ascending order

## Data
```csv
New York , 10.0  
New York , 12.0 
Los Angeles , 20.0  
Los Angeles , 22.0 
San Francisco , 15.0  
San Francisco , 18.0
```

## Metadata- columns
city - String  
temperature - Double  
