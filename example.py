from magictables import MagicTable


API_KEY = "1865f43a0549ca50d341dd9ab8b29f49"


def main():
    # Create MagicTable instances for each API endpoint
    popular_movies = MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    )

    # Get the first movie ID from the popular movies list
    first_movie_id = popular_movies["id"][0]

    # Fetch movie details for the first movie
    movie_details = MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/{first_movie_id}?api_key={API_KEY}"
    )

    # Get the first production company ID from the movie details
    first_company_id = movie_details["production_companies"][0]["id"]

    # Fetch company details
    company_details = MagicTable.from_api(
        f"https://api.themoviedb.org/3/company/{first_company_id}?api_key={API_KEY}"
    )

    # Get the origin country of the company
    origin_country = company_details["origin_country"]

    # Fetch country details
    country_details = MagicTable.from_api(
        f"https://restcountries.com/v3.1/alpha/{origin_country}"
    )

    print("Popular Movies:")
    print(popular_movies)

    print("\nMovie Details:")
    print(movie_details)

    print("\nCompany Details:")
    print(company_details)

    print("\nCountry Details:")
    print(country_details)

    # Example of using join_with_query
    movie_analysis = popular_movies.join_with_query(
        "Find movies with a vote average greater than 7.5"
    )

    print("\nMovie Analysis:")
    print(movie_analysis)

    # Save the results to a JSON file
    movie_analysis.write_json("movies_analysis.json")


if __name__ == "__main__":
    main()
