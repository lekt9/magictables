import os
import asyncio
from magictables import MagicTable
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the TMDb API key
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# Base URL for TMDb API
BASE_URL = "https://api.themoviedb.org/3"

async def main():
    # 1. Start with the first page of popular movies
    initial_url = f"{BASE_URL}/movie/popular?api_key={TMDB_API_KEY}&page=1"
    popular_movies = await MagicTable.from_api(initial_url)

    # 2. Chain requests to get all pages of popular movies
    all_pages = await popular_movies.gen(
        f"Get pages 1 to 100",
    )

    print("All pages of popular movies:")
    print(all_pages.head())
    print(f"Total movies fetched: {len(all_pages)}")
    # 3. Chain requests to get detailed information for each movie
    movie_list = await all_pages.chain(
        f"{BASE_URL}/discover/movie?api_key={TMDB_API_KEY}&sort_by=popularity.desc&page={{page}}"
    )

    # 3. Chain requests to get detailed information for each movie
    movie_details = await movie_list.chain(
        f"{BASE_URL}/movie/{{id}}?api_key={TMDB_API_KEY}"
    )

    print("\nDetailed movie information:")
    print(movie_details.head())

    # 4. Chain requests to get credits for each movie
    movie_credits = await movie_details.chain(
        f"{BASE_URL}/movie/{{id}}/credits?api_key={TMDB_API_KEY}"
    )

    print("\nMovie credits:")
    print(movie_credits.head())

    # 6. Transform the data to create a summary
    summary = await movie_credits.transform("""
    Select 
        id, 
        title, 
        release_date, 
        vote_average as rating, 
        budget, 
        revenue, 
        (revenue - budget) as profit
    Order by profit Desc
    Limit 10
    """)

    print("\nTop 10 movies by profit:")
    print(summary)

# Run the main function
asyncio.run(main())