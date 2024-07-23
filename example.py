from magictables import MagicTable

API_KEY = "1865f43a0549ca50d341dd9ab8b29f49"


def main():
    # Create MagicTable instances for each API endpoint
    popular_movies = MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    )

    print("Popular Movies:")
    print(popular_movies)

    # Example of using join_with_query
    high_rated_movies = popular_movies.join_with_query(
        "Find movies with a vote average greater than 7.5"
    )

    print("\nHigh Rated Movies:")
    print(high_rated_movies)

    # Get the first movie ID from the high-rated movies list
    first_movie_id = high_rated_movies["id"][0]

    # Fetch movie details for the first high-rated movie
    movie_details = MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/{first_movie_id}?api_key={API_KEY}"
    )

    print("\nMovie Details:")
    print(movie_details)

    # Get the first production company ID from the movie details
    first_company_id = movie_details["production_companies"][0]["id"]

    # Fetch company details
    company_details = MagicTable.from_api(
        f"https://api.themoviedb.org/3/company/{first_company_id}?api_key={API_KEY}"
    )

    print("\nCompany Details:")
    print(company_details)

    # Get the origin country of the company
    origin_country = company_details["origin_country"]

    # Fetch country details
    country_details = MagicTable.from_api(
        f"https://restcountries.com/v3.1/alpha/{origin_country}"
    )

    print("\nCountry Details:")
    print(country_details)

    # Example of using from_query
    action_movies = MagicTable.from_query(
        "Find action movies released in the last year with a vote average greater than 7.0"
    )

    print("\nAction Movies:")
    print(action_movies)

    # Save the results to a JSON file
    action_movies.write_json("action_movies.json")


if __name__ == "__main__":
    main()
