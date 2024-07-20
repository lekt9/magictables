import requests
from magictables import mtable, create_chain
import polars as pl

API_KEY = "1865f43a0549ca50d341dd9ab8b29f49"


# Step 1: Fetch popular movies from TMDB
@mtable(query="Get popular movies with their id, title, and release date")
def get_popular_movies() -> pl.DataFrame:
    url = "https://api.themoviedb.org/3/movie/popular"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    data = response.json()["results"][:5]  # Limit to 5 movies for this example
    return pl.DataFrame(data)


# Step 2: Fetch movie details including production companies
@mtable(query="Get movie details including production companies")
def get_movie_details(movies: pl.DataFrame) -> pl.DataFrame:
    def fetch_movie_details(movie_id):
        url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        params = {"api_key": API_KEY}
        response = requests.get(url, params=params)
        return response.json()

    return movies.with_columns(
        pl.col("id").map_elements(fetch_movie_details).alias("details")
    )


# Step 3: Fetch company details
@mtable(query="Get company details including origin country")
def get_company_details(movies: pl.DataFrame) -> pl.DataFrame:
    def fetch_company_details(company_id):
        url = f"https://api.themoviedb.org/3/company/{company_id}"
        params = {"api_key": API_KEY}
        response = requests.get(url, params=params)
        return response.json()

    company_ids = (
        movies["production_companies"]
        .explode()
        .map_elements(lambda x: x["id"])
        .unique()
    )
    return pl.DataFrame({"id": company_ids}).with_columns(
        pl.col("id").map_elements(fetch_company_details).alias("details")
    )


# Step 4: Fetch country details
@mtable(query="Get country details including population and capital")
def get_country_details(companies: pl.DataFrame) -> pl.DataFrame:
    def fetch_country_details(country_code):
        url = f"https://restcountries.com/v3.1/alpha/{country_code}"
        response = requests.get(url)
        return response.json()[0]

    country_codes = companies["origin_country"].unique()
    return pl.DataFrame({"country_code": country_codes}).with_columns(
        pl.col("country_code").map_elements(fetch_country_details).alias("details")
    )


# Step 5: Generate insights
@mtable(
    query="Generate insights about movies, production companies, and their countries"
)
def generate_insights(data: pl.DataFrame) -> pl.DataFrame:
    def create_insight(row):
        movie = row["movie"]
        companies = row["companies"]
        countries = row["countries"]

        insight = f"The movie '{movie['title']}' was produced by "
        company_names = [company["name"] for company in companies]
        insight += ", ".join(company_names[:-1]) + f" and {company_names[-1]}. "

        for company in companies:
            country = next(
                (c for c in countries if c["cca2"] == company["origin_country"]), None
            )
            if country:
                insight += (
                    f"{company['name']} is based in {country['name']['common']}, "
                )
                insight += f"which has a population of {country['population']} "
                insight += f"and its capital is {country['capital'][0]}. "

        return insight

    return data.with_columns(
        pl.struct(["movie", "companies", "countries"])
        .map_elements(create_insight)
        .alias("insight")
    )


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
    for row in result.to_dicts():
        print(f"\nMovie: {row['movie']['title']}")
        print(f"Insight: {row['insight']}")

    print("\nSecond execution (cache hit expected):")
    result2 = movie_analysis_chain.execute(
        {}
    )  # Pass an empty dictionary as initial input
    print("Cache hit successful" if result.equals(result2) else "Cache miss")


if __name__ == "__main__":
    main()
