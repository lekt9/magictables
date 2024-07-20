import requests
from magictables import mtable, create_chain

API_KEY = "1865f43a0549ca50d341dd9ab8b29f49"


@mtable(query="Get popular movies with their id, title, and release date")
def get_popular_movies(input_data=None):
    url = "https://api.themoviedb.org/3/movie/popular"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    return response.json()


@mtable(query="Get movie details including production companies")
def get_movie_details(movie_id: int):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    return response.json()


@mtable(query="Get company details including origin country")
def get_company_details(company_id: int):
    url = f"https://api.themoviedb.org/3/company/{company_id}"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    return response.json()


@mtable(query="Get country details including population and capital")
def get_country_details(origin_country: str):
    url = f"https://restcountries.com/v3.1/alpha/{origin_country}"
    response = requests.get(url)
    return response.json()


@mtable(
    query="Generate insights about movies, production companies, and their countries"
)
def generate_insights(movie, companies, countries):
    insight = f"The movie '{movie['title']}' was produced by "
    company_names = [company["name"] for company in companies]
    insight += ", ".join(company_names[:-1]) + f" and {company_names[-1]}. "

    for company in companies:
        country = countries.get(company["origin_country"])
        if country:
            insight += f"{company['name']} is based in {country['name']['common']}, "
            insight += f"which has a population of {country['population']} "
            insight += f"and its capital is {country['capital'][0]}. "

    return {"movie": movie["title"], "insight": insight}


def main():
    movie_analysis_chain = create_chain(
        get_popular_movies,
        get_movie_details,
        get_company_details,
        # generate_insights,
    )

    print("Executing Movie Analysis chain:")
    result = movie_analysis_chain.execute({})

    print("\nSecond execution (cache hit expected):")
    result2 = movie_analysis_chain.execute({})
    print("Cache hit successful" if result.equals(result2) else "Cache miss")


if __name__ == "__main__":
    main()
