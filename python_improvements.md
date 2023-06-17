Improvements

If I could spend more time on the task and would aim at running the script daily
- Split, make the main file more lean, try to create and use Classes
- Use mutli threading to query and parse results from different pages in parallel
- Better error handling
- Add timestamps with the time when the data was processed in the datasets
- See if we can query the API based on last_modified timestamp to only reprocess changed records since last script run
- Add data validation, tests, make sure we get all the category's playstists and information
- For multiple categories store in a list and use a loop
- Add timestamp in dataset names to avoid overwritting