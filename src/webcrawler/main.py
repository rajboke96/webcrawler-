import asyncio
from async_webcrawler import main

if __name__ == '__main__':
    url="https://www.w3schools.com/"
    asyncio.run(main(url=url))