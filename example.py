from magictables import Source, Chain, Explain, to_dataframe, Input

# Define data sources
github_api = Source.api("GitHub API")
github_api.add_route(
    "users",
    "https://api.github.com/users/{username}",
    query="Get user data including name, bio, and public repos count",
)
github_api.add_route(
    "repos",
    "https://api.github.com/users/{username}/repos",
    query="Get user's repositories with star counts and languages used",
)

github_web = Source.web(
    "GitHub Web", query="Extract user's contribution graph data for the last year"
)

user_pdf = Source.pdf(
    "User Reports", query="Extract user's project summaries and completion dates"
)

# Create a data processing chain
user_analysis = Chain()
user_analysis.add(github_api.route("users"))
user_analysis.add(github_api.route("repos"))
user_analysis.add(github_web)
user_analysis.add(user_pdf)

# AI-driven data joining and analysis
user_analysis.analyze(
    "Combine API user data, web-scraped contribution data, and PDF report data"
)

# Execute the chain with batch processing
usernames = ["octocat", "torvalds", "gvanrossum"]
result = user_analysis.execute(usernames=usernames, batch_size=2)

# Convert to Polars DataFrame
df = to_dataframe(result)
print("Result DataFrame:")
print(df)

# Explain the data flow and processing
explanation = Explain(user_analysis)
print("\nChain Summary:")
print(explanation.summary())
print("\nData Flow:")
print(explanation.data_flow())
print("\nParsing Logic:")
print(explanation.parsing_logic())
print("\nShadow DB Schema:")
print(explanation.shadow_db_schema())

# Demonstrate Input class usage
input_handler = Input()

# Fetch data from API
api_data = input_handler.from_api("GitHub API", "users", username="octocat")
print("\nAPI Data:")
print(api_data)

# Fetch data from web
web_data = input_handler.from_web("https://github.com/octocat")
print("\nWeb Data:")
print(web_data)
