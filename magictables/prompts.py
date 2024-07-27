TRANSFORM_PROMPT = """Given the following pandas DataFrame structure and the query, generate Python code to process or analyze the data using pandas.

What we are trying to achieve: {query}
Current DataFrame you must only work with:
DataFrame Structure:
{data}
Please provide Python code to process this DataFrame, adhering to the following guidelines:
1. Only use columns that exist in the DataFrame. Do not reference any columns not listed above.
2. Ensure all operations are efficient and use pandas vectorized operations where possible.
3. Handle potential data type issues, especially for date/time columns or numeric calculations.
4. The code should return a pandas DataFrame as the result.
5. Do not include any print statements or comments in the code.
6. The input DataFrame is named 'df'.
7. If the DataFrame is empty or missing required columns, create a sample DataFrame with the necessary columns.
8. When working with dates, always use pd.to_datetime() for conversion and handle potential errors.
9. For age calculations, use a method that works across different pandas versions, avoiding timedelta conversions to years.
10. You MUST only use pandas and no other libraries.
11. Do NOT handle mempty dataframes or missing columns.
12. You can only import pandas as pd, no other libraries.
IMPORTANT: df is the variable that is your input dataframe. It is defined outside of the code you are writing. Do not recreate the data as it has already been defined.

Your response should be in the following format, here are some EXAMPLES of python code. do NOT use it directly, but rather write code for the dataframe structure provided above:
```python
df = ...
```
"""


GENERATE_DATAFRAME_PROMPT = """
Generate Python code to create a pandas DataFrame using iterators or generators based on the following natural language query:

"{query}"

The code should use pandas to efficiently create the DataFrame.
Only import and use pandas (as pd). Do not use any other libraries.

If the query mentions or implies the need for pagination or fetching data from an API,
generate query parameter pairs based on the URL template, focusing on pagination requirements.

Your response should be in the following format, here are some EXAMPLES of python code. do NOT use it directly, but rather write code for the query provided above:
```python
1. Query: "Fetch top 100 movies from TMDB API with title, release date, and rating. Use this URL template: https://api.themoviedb.org/3/movie/top_rated?api_key={{api_key}}&page={{page}}"
Expected output:

import pandas as pd

def generate_query_params():
    for page in range(1, 6):  # Assuming 20 movies per page, 5 pages for 100 movies
        yield {{
            'api_key': 'dummy_api_key',
            'page': page
        }}

result = pd.DataFrame(generate_query_params())
```
2. Query: "Fetch weather data for New York for the first week of January 2024, using a weather API with pagination. Use this URL template: https://api.example.com/weather?city={{city}}&date={{date}}&page={{page}}"
Expected output:
```python
import pandas as pd

def generate_query_params():
    city = 'New York'
    date_range = pd.date_range(start='2024-01-01', end='2024-01-07')
    for date in date_range:
        yield {{
            'city': city,
            'date': date.strftime('%Y-%m-%d'),
            'page': 1  # Assuming one page per day
        }}

result = pd.DataFrame(generate_query_params())
```
"""


IDENTIFY_KEY_COLUMNS_PROMPT = """Given the following API URL template and the current DataFrame structure, 
identify the most suitable columns to use as keys for chaining API calls.

API URL Template: {api_url_template}

Placeholders: {placeholders}

DataFrame Columns and Types:
{column_info}

Please provide a mapping of placeholders to the most suitable column names from the DataFrame.
If a placeholder doesn't have a matching column, map it to null.
Your response should be in the following JSON format:
{{
    "column_mapping": [
        {{"placeholder": "example_placeholder1", "column": "example_column1"}},
        {{"placeholder": "example_placeholder2", "column": "example_column2"}},
        {{"placeholder": "placeholder_without_match", "column": null}}
    ]
}}
Replace the example placeholders and column names with the actual ones you identify as the best matches.
Ensure that ALL placeholders from the API URL template are accounted for in your response.
"""

GENERATE_DATAFRAME_PROMPT = """
Generate Python code to create a pandas DataFrame using iterators or generators based on the following natural language query:

"{query}"

The code should use pandas to efficiently create the DataFrame.
Only import and use pandas (as pd). Do not use any other libraries.

If the query mentions or implies the need for pagination or fetching data from an API,
generate query parameter pairs based on the URL template, focusing on pagination requirements.

Your response should be in the following format, here are some EXAMPLES of python code. do NOT use it directly, but rather write code for the query provided above:
```python
1. Query: "Fetch top 100 movies from TMDB API with title, release date, and rating. Use this URL template: https://api.themoviedb.org/3/movie/top_rated?api_key={{api_key}}&page={{page}}"
Expected output:

import pandas as pd

def generate_query_params():
    for page in range(1, 6):  # Assuming 20 movies per page, 5 pages for 100 movies
        yield {{
            'api_key': 'dummy_api_key',
            'page': page
        }}

result = pd.DataFrame(generate_query_params())
```
2. Query: "Fetch weather data for New York for the first week of January 2024, using a weather API with pagination. Use this URL template: https://api.example.com/weather?city={{city}}&date={{date}}&page={{page}}"
Expected output:
```python
import pandas as pd

def generate_query_params():
    city = 'New York'
    date_range = pd.date_range(start='2024-01-01', end='2024-01-07')
    for date in date_range:
        yield {{
            'city': city,
            'date': date.strftime('%Y-%m-%d'),
            'page': 1  # Assuming one page per day
        }}

result = pd.DataFrame(generate_query_params())
```
"""
