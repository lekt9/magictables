import requests
from magictables import mtable, create_chain
import polars as pl

API_KEY = "1865f43a0549ca50d341dd9ab8b29f49"


# Step 1: Fetch popular movies from TMDB
@mtable(query="Get popular movies with their id, title, and release date")
def get_popular_movies(input_data=None) -> pl.DataFrame:
    url = "https://api.themoviedb.org/3/movie/popular"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    data = response.json()["results"][:5]  # Limit to 5 movies for this example
    return pl.DataFrame(data)


# Step 2: Fetch movie details including production companies
@mtable(query="Get movie details including production companies")
def get_movie_details(movies: pl.DataFrame) -> pl.DataFrame:
    details = []
    print("movies", movies)
    for movie_id in movies["id"]:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        params = {
            "api_key": API_KEY
        }  # This is a dummy key, replace with a real one if needed
        response = requests.get(url, params=params)
        details.append(response.json())
    return pl.DataFrame(details)


# Step 3: Fetch company details
@mtable(query="Get company details including origin country")
def get_company_details(movies: pl.DataFrame) -> pl.DataFrame:
    company_ids = set()
    for companies in movies["production_companies"]:
        company_ids.update(company["id"] for company in companies)

    details = []
    for company_id in company_ids:
        url = f"https://api.themoviedb.org/3/company/{company_id}"
        params = {
            "api_key": API_KEY
        }  # This is a dummy key, replace with a real one if needed
        response = requests.get(url, params=params)
        details.append(response.json())
    return pl.DataFrame(details)


# Step 4: Fetch country details
@mtable(query="Get country details including population and capital")
def get_country_details(companies: pl.DataFrame) -> pl.DataFrame:
    country_codes = companies["origin_country"].unique().to_list()
    details = []
    for country_code in country_codes:
        url = f"https://restcountries.com/v3.1/alpha/{country_code}"
        response = requests.get(url)
        details.append(response.json()[0])
    return pl.DataFrame(details)


# Step 5: Generate insights
@mtable(
    query="Generate insights about movies, production companies, and their countries"
)
def generate_insights(data: pl.DataFrame) -> pl.DataFrame:
    insights = []
    for movie in data.to_dicts():
        insight = f"The movie '{movie['title']}' was produced by "
        companies = [company["name"] for company in movie["production_companies"]]
        insight += ", ".join(companies[:-1]) + f" and {companies[-1]}. "
        for company in movie["production_companies"]:
            country = company["origin_country"]
            if country in data["countries"].column("cca2"):
                country_info = (
                    data["countries"].filter(pl.col("cca2") == country).to_dicts()[0]
                )
                insight += (
                    f"{company['name']} is based in {country_info['name']['common']}, "
                )
                insight += f"which has a population of {country_info['population']} "
                insight += f"and its capital is {country_info['capital'][0]}. "
        insights.append({"movie": movie["title"], "insight": insight})
    return pl.DataFrame(insights)


def main():
    # Create a chain of functions using create_chain
    movie_analysis_chain = create_chain(
        get_popular_movies,
        get_movie_details,
        get_company_details,
        get_country_details,
        generate_insights,
    )

    # Execute the chain
    print("Executing Movie Analysis chain:")
    result = movie_analysis_chain.execute(
        {}
    )  # Pass an empty dictionary as initial input

    # Display results
    print("\nAnalysis Results:")
    for insight in result.to_dicts():
        print(f"\nMovie: {insight['movie']}")
        print(f"Insight: {insight['insight']}")

    print("\nSecond execution (cache hit expected):")
    result2 = movie_analysis_chain.execute(
        {}
    )  # Pass an empty dictionary as initial input
    print("Cache hit successful" if result.equals(result2) else "Cache miss")


if __name__ == "__main__":
    main()
