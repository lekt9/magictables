from magictables import Source, Chain

API_KEY = "1865f43a0549ca50d341dd9ab8b29f49"

# Define data sources
tmdb_api = Source.api("TMDB API")
tmdb_api.add_route(
    "popular_movies",
    f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}",
    "Get popular movies",
)
tmdb_api.add_route(
    "movie_details",
    f"https://api.themoviedb.org/3/movie/{{movie_id}}?api_key={API_KEY}",
    "Get movie details",
)
tmdb_api.add_route(
    "company_details",
    f"https://api.themoviedb.org/3/company/{{company_id}}?api_key={API_KEY}",
    "Get company details",
)

countries_api = Source.api("Countries API")
countries_api.add_route(
    "country_details",
    "https://restcountries.com/v3.1/alpha/{origin_country}",
    "Get country details",
)

# Create a Chain
movie_analysis = Chain()
movie_analysis.add(tmdb_api, "Fetch popular movies", "popular_movies")
movie_analysis.add(tmdb_api, "Fetch movie details", "movie_details")
movie_analysis.add(tmdb_api, "Fetch company details", "company_details")
movie_analysis.add(countries_api, "Fetch country details", "country_details")

# Add analysis step
# movie_analysis.analyze(
#     "Generate insights about movies, production companies, and their countries"
# )


def main():
    # Create an input DataFrame with initial data

    print("Executing Movie Analysis chain:")
    result = movie_analysis.execute()

    print("\nResult:")

    print("\nSecond execution (cache hit expected):")
    result2 = movie_analysis.execute()

    result2.write_json("movies.json")

    print("Result", result2)


if __name__ == "__main__":
    main()
