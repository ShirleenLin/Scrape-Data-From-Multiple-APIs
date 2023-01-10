I used to iterate through stocks VERSUS Now I iterate through trade_dates, and download all stocks’ info each day as a dataframe, and THEN pandas concat all DataFrames 

Intuitively, it should be much faster - it turned out to be tremendously faster

The code file below is what I wrote for multiple APIs again.
