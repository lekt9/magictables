import os
import requests
import magictables
from dotenv import load_dotenv

load_dotenv()


@magictables.mtable()
def get_github_user(username: str):
    response = requests.get(f"https://api.github.com/users/{username}")
    return response.json()


# First call (will hit the API and cache the result)
result_df = get_github_user("lewistham9x")
print(result_df)

# Second call (will retrieve from cache)
cached_result_df = get_github_user("octocat")
print(cached_result_df)


# Example usage
@magictables.mchat(
    api_key=os.getenv("OPENAI_API_KEY", ""),
    model=os.getenv(
        "OPENAI_BASE_MODEL",
        "meta-llama/llama-3-70b-instruct",
    ),
    base_url=os.getenv(
        "OPENAI_BASE_URL",
        "https://openrouter.ai/api/v1/chat/completions",
    ),
)
def cs_playstyle(csgo_player_name: str, playstyle: str):
    playstyle = playstyle * 2
    return playstyle


# Example call
df = cs_playstyle(
    csgo_player_name="s1mple",
    playstyle=None,
)
print(df)
