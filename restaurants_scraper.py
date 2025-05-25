import asyncio
import pandas as pd
import random
import time
import math
import csv
import os
from urllib.parse import urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

PROXY = 'http://brd-customer-hl_00521d02-zone-tripadvisor_pt-country-pt:lskr16h8nucn@brd.superproxy.io:33335'


def parse_proxy(proxy_url):
    parsed = urlparse(proxy_url)
    return {
        "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
        "username": parsed.username,
        "password": parsed.password
    }


def generate_url(restaurant_id, page_number):
    base = f'https://www.tripadvisor.pt/Restaurant_Review-{restaurant_id}'
    if page_number == 1:
        return f'{base}-Reviews.html'
    offset = (page_number - 1) * 15
    return f'{base}-Reviews-or{offset}.html'


async def get_page_content(page, url):
    try:
        await page.goto(url, timeout=60000)

        try:
            await page.locator("button#onetrust-accept-btn-handler").click(timeout=5000)
            print("✅ Pop-up de cookies aceite.")
        except:
            print("ℹ️ Sem pop-up de cookies.")

        await page.mouse.wheel(0, 300)
        await page.wait_for_timeout(random.randint(2000, 4000))

        return await page.content(), page

    except PlaywrightTimeoutError:
        print(f"🚫 Timeout ao aceder à página: {url}")
        return None, None
    except Exception as e:
        print(f"❌ Erro inesperado ao carregar {url}: {str(e)}")
        return None, None


async def main():
    from bs4 import BeautifulSoup

    async with async_playwright() as p:
        proxy_settings = parse_proxy(PROXY)
        browser = await p.chromium.launch(headless=False, proxy=proxy_settings)
        context = await browser.new_context(
            locale="pt-PT",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            ignore_https_errors=True
        )
        page = await context.new_page()

        output_filename = 'tripadvisor_reviews.csv'
        with open('scraper_tripadvisor/porto_rest_ids.txt', 'r', encoding='utf-8') as file:
            restaurant_ids = [line.strip() for line in file if line.strip()]

        count = 0

        for restaurant_id in restaurant_ids:
            count += 1
            print(f"\n➡️ A aceder ao restaurante: {restaurant_id} (faltam {len(restaurant_ids)-count})")

            current_page = 1
            restaurant_info = {'id': restaurant_id, 'name': ''}

            while True:
                if current_page > 50:
                    print(f"🚫 Limite de 50 páginas atingido para {restaurant_id}. A passar ao próximo restaurante.")
                    break

                url = generate_url(restaurant_id, current_page)
                print(f"Scraping page {current_page} of {restaurant_id}...")

                html, page_ref = await get_page_content(page, url)
                if not html:
                    print("⚠️ Conteúdo não carregado. A saltar página.")
                    break

                soup = BeautifulSoup(html, 'html.parser')

                if restaurant_info['name'] == '':
                    try:
                        name = soup.find('h1').text.strip()
                        restaurant_info['name'] = name
                        results_text_container = soup.find('div', class_='biGQs _P pZUbB hmDzD')
                        if results_text_container:
                            numbers = [int(s) for s in results_text_container.text.split() if s.isdigit()]
                            total_reviews = numbers[-1] if numbers else 0
                            last_page = math.ceil(total_reviews / 15)
                            print(f"Total pages: {last_page}")
                        else:
                            last_page = None
                    except:
                        last_page = None

                review_cards = soup.find_all('div', attrs={'data-automation': 'reviewCard'})
                if not review_cards:
                    print("Sem reviews nesta página. A terminar.")
                    break

                reviews = []
                for i, review in enumerate(review_cards):
                    review_data = {
                        'reviewer_id': '',
                        'reviewer_contributions': '',
                        'rating': '',
                        'title': '',
                        'text': '',
                        'date': ''
                    }

                    user_info_container = review.find('div', class_='QIHsu Zb')
                    if user_info_container:
                        user_link = user_info_container.find('a', href=True)
                        if user_link and '/Profile/' in user_link['href']:
                            review_data['reviewer_id'] = user_link['href'].split('/Profile/')[-1]

                        contrib_span = user_info_container.find('span', class_='b')
                        if contrib_span and contrib_span.text.strip().isdigit():
                            review_data['reviewer_contributions'] = contrib_span.text.strip()

                    rating_element = review.find('svg', class_='evwcZ')
                    if rating_element:
                        review_data['rating'] = rating_element.find('title').text.strip().replace(' de 5 bolhas', '')

                    title_element = review.find('div', attrs={'data-test-target': 'review-title'})
                    if title_element:
                        review_data['title'] = title_element.text.strip()

                    text_element = review.find('div', attrs={'data-test-target': 'review-body'})
                    if text_element:
                        review_data['text'] = text_element.text.strip().replace('Ler mais', '')

                    # ➕ nova forma de extrair a data com Playwright
                    try:
                        data_locator = page_ref.locator("div.JVaPo.Gi.kQjeB").nth(i).locator("div.TgEgi").locator("span")
                        spans = await data_locator.all_text_contents()
                        for span in spans:
                            if "Data da visita" in span:
                                idx = spans.index(span)
                                review_data['date'] = spans[idx + 1] if idx + 1 < len(spans) else ''
                                break
                    except:
                        review_data['date'] = ''

                    reviews.append(review_data)
                    time.sleep(1)

                file_exists = os.path.exists(output_filename)
                with open(output_filename, mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if not file_exists:
                        writer.writerow([
                            'RESTAURANT_ID', 'RESTAURANT_NAME', 'RATING', 'REVIEW_TITLE', 'REVIEW_DETAILS', 'REVIEW_DATE', 'USER_ID', 'USER_CONTRIBUTIONS'])

                    for review in reviews:
                        writer.writerow([
                            restaurant_info['id'],
                            restaurant_info['name'],
                            review['rating'],
                            review['title'],
                            review['text'],
                            review['date'],
                            review['reviewer_id'],
                            review['reviewer_contributions']
                        ])

                print(f"Página {current_page} de {restaurant_id} concluída.")
                if last_page and current_page >= last_page:
                    print(f"✅ Concluído restaurante {restaurant_id}")
                    break

                current_page += 1
                await page.wait_for_timeout(random.randint(6000, 10000))

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
