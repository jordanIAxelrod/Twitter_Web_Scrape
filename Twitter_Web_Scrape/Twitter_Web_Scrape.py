import requests
from bs4 import BeautifulSoup
import argparse
import pandas as pd
import time
import multiprocessing as mp
import asyncio
from aiohttp import ClientSession
import numpy as np

# Grab all links that have twitter in them
async def get_twitter_link(page):
    try:
        return await [a['href'] for a in soup.find_all('a', href=True) if 'twitter' in a['href']]
    except:
        return None

# Grab the web page as a soup
async def get_soup(row, session):
    url = row['website']
    if pd.notna(url):
        print(url)
        try:
            async with session.get(url) as resp:
                try:
                    soup = BeautifulSoup(await resp.text(), 'lxml')
                    row['twitter'] = await get_twitter_link(soup)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)





# Add all identifiable twitters from the links
async def add_twitter_to_df(df):
    df.twitter = None
    tasks = []
    sem = asyncio.Semaphore(10)
    async with ClientSession(trust_env=True) as session:

        for i in df.index:

            tasks.append(
                asyncio.create_task(
                    get_soup(
                        df.loc[i], 
                        session
                        )
                    )
                
                )
        await asyncio.gather(*tasks)
    return df

def twitter_scrape(url):
    
    frame = pd.read_csv(url)

    frame = asyncio.run(add_twitter_to_df(frame))

    new_url = '2021.10.19 police1-companies-with-twit-and-addresses.csv'
    frame.to_csv(new_url)
    return frame


   




def main(url):
    start = time.time()
    frame = twitter_scrape(url)
    delta = time.time() - start
    print(frame[frame['website'] != None])
    print(f'scraping completed! runtime = {delta:.2f} seconds')

if __name__ == '__main__':
    url = 'police1-companies-with-addresses.csv'
    main(url)
