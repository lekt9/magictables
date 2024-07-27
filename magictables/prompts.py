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
12. Handle strange types like datetime by converting them into something pandas compatible first.

IMPORTANT: df is the variable that is your input dataframe.

Your response should be in the following format, here are some EXAMPLES of python code. do NOT use it directly, but rather write code for the dataframe structure provided above:
```python
date_columns = df.select_dtypes(include=['object']).columns[df.select_dtypes(include=['object']).apply(lambda x: pd.to_datetime(x, errors='coerce').notnull().all())]
for col in date_columns:
df[col] = pd.to_datetime(df[col], errors='coerce')

# Handle missing values
df = df.fillna({{
col: df[col].mean() if pd.api.types.is_numeric_dtype(df[col]) else df[col].mode()[0]
for col in df.columns
}})

# Create age column if 'birthdate' exists
if 'birthdate' in df.columns:
df['age'] = (pd.Timestamp.now() - df['birthdate']).astype('<m8[Y]').astype(int)

# Perform string operations
text_columns = df.select_dtypes(include=['object']).columns
for col in text_columns:
df[col] = df[col].str.strip().str.lower()

# One-hot encode categorical variables
categorical_columns = df.select_dtypes(include=['object']).columns
df = pd.get_dummies(df, columns=categorical_columns, drop_first=True)

# Scale numerical columns
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
df[numeric_columns] = scaler.fit_transform(df[numeric_columns])

# Perform aggregations
if 'category' in df.columns and 'value' in df.columns:
df['total_value'] = df.groupby('category')['value'].transform('sum')
df['percentage'] = df['value'] / df['total_value'] * 100

# Filter data based on conditions
if 'age' in df.columns:
df = df[df['age'] >= 18]

# Sort the dataframe
if 'date' in df.columns:
df = df.sort_values('date', ascending=False)

# Rename columns for clarity
df = df.rename(columns={{
'col1': 'feature_1',
'col2': 'feature_2'
}})

# Create a new calculated column
if 'price' in df.columns and 'quantity' in df.columns:
df['total_revenue'] = df['price'] * df['quantity']

# Perform a rolling average if time-series data is present
if 'date' in df.columns and 'value' in df.columns:
df = df.sort_values('date')
df['rolling_avg'] = df['value'].rolling(window=7).mean()

# Drop unnecessary columns
columns_to_drop = ['temp_col1', 'temp_col2']
df = df.drop(columns=columns_to_drop, errors='ignore')

# Reset index if needed
df = df.reset_index(drop=True)

result = df
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

Please provide the names of the columns that best match the placeholders in the API URL template.
Your response should be in the following JSON format:
{{"column_names": ["example_column1", "example_column2"]}}
Replace the example column names with the actual column names you identify as the best matches.
"""
