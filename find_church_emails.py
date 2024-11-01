# %%
import pandas as pd
import aiohttp
import asyncio
import logging
from tqdm import tqdm
from cachetools import cached, TTLCache
from bs4 import BeautifulSoup
import re
import requests

# %%

# Set up logging
logging.basicConfig(filename="url_log.txt", level=logging.INFO)

# Cache setup
cache = TTLCache(maxsize=1000, ttl=300)

# Email regex pattern
EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"


@cached(cache)
async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return ""


async def get_contact_page_urls(
    session: aiohttp.ClientSession, main_url: str, contact_page_keywords: list[str]
) -> list[str]:
    try:
        html_content = await fetch(session, main_url)
        soup = BeautifulSoup(html_content, "lxml")
        contact_urls = [
            link["href"]
            for link in soup.find_all("a", href=True)
            if any(keyword in link["href"].lower() for keyword in contact_page_keywords)
        ]
        logging.info(f"Found URLs for {main_url}: {contact_urls}")
        return contact_urls
    except Exception as e:
        logging.error(f"Error fetching {main_url}: {e}")
        return [str(e)]


async def extract_emails_from_page(
    session: aiohttp.ClientSession, url: str, email_regex: str
) -> list[str]:
    try:
        html_content = await fetch(session, url)
        emails = re.findall(email_regex, html_content)
    except Exception as e:
        logging.error(f"Error extracting emails from {url}: {e}")
        emails = [str(e)]
    return emails


async def process_kerk_url(
    session: aiohttp.ClientSession,
    kerk_url: str,
    contact_page_keywords: list[str],
    email_regex: str,
) -> tuple[str, list[str]]:
    contact_pages = await get_contact_page_urls(
        session, kerk_url, contact_page_keywords
    )
    emails = [
        await extract_emails_from_page(session, page, email_regex)
        for page in contact_pages
    ]

    return kerk_url, emails


def is_url(url: str) -> bool:
    if url:
        url_regex = re.compile(r"https?://[^\s/$.?#].[^\s]*")
        return bool(url_regex.match(url))
    else:
        return False


async def main(
    kerk_urls: list[str], contact_page_keywords: list[str], email_regex: str
) -> dict[str, list[str]]:
    kerk_emails = {}
    async with aiohttp.ClientSession() as session:
        tasks = [
            process_kerk_url(session, kerk_url, contact_page_keywords, email_regex)
            for kerk_url in kerk_urls
        ]
        # WITH PROGRESS BAR
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            kerk_url, emails = await f
            kerk_emails[kerk_url] = emails
        # WITHOUT PROGRESS BAR (but likely faster)
        # results = await asyncio.gather(*tasks)
        # kerk_emails = {kerk_url: emails for kerk_url, emails in results}
    return kerk_emails


# %%
# Fetch church URLs
pkn_kerken = requests.get("https://protestantsekerk.nl/kerkzoeker/?json").json()[
    "churches"
]
contact_page_keywords = [
    "contact",
    "about",
    "over ons",
    "anbi",
    "gegevens",
    "info",
    "wie",
    "geven",
    "give",
    "gift",
    "donatie",
    "doneren",
    "doneer",
    "mail",
]
kerk_urls = [kerk["website"] for kerk in pkn_kerken if is_url(kerk["website"])]

# %%

# Run the main function
kerk_emails = asyncio.run(main(kerk_urls, contact_page_keywords, EMAIL_REGEX))

# %%
# Prepare data for DataFrame
rows = [
    (key, value) if values else (key, None)
    for key, values in kerk_emails.items()
    for value in values
]

# Create DataFrame
df = pd.DataFrame(rows, columns=["url", "email"]).explode("email")
df = df[df["email"].str.contains(EMAIL_REGEX, na=False)]
df = df.drop_duplicates(subset=["email"])
df.to_excel("kerk_emails_via_contactpagina.xlsx")
