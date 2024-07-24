from magictables import MagicTable
import os
from dotenv import load_dotenv
import asyncio

from magictables.notsomagictables import NotSoMagicTable

load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")


async def main():
    # Create a MagicTable instance
    mt = MagicTable()
    # await mt.clear_all_data()

    # Fetch popular movies
    # print("\nFetching popular movies...")
    popular_movies = await MagicTable.from_api(
        f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}"
    )
    # print(popular_movies)

    # Chain API calls for movie details
    # print("\nChaining API calls for movie details...")
    movie_details = await popular_movies.chain(
        api_url=f"https://api.themoviedb.org/3/movie/{{id}}?api_key={API_KEY}",
    )
    # print(movie_details)
    # Example of using from_query to combine data from multiple chained calls
    # print("\nQuerying across chained data...")
    result = await movie_details.transform(
        "Find popular movies with a vote average greater than 7.5"
    )
    # print(result)


if __name__ == "__main__":
    asyncio.run(main())
