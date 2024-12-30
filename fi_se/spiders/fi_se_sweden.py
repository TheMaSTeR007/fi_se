from urllib.parse import urlencode
from scrapy.cmdline import execute
from lxml.html import fromstring
from typing import Iterable
from scrapy import Request
import pandas as pd
import unicodedata
import random
import scrapy
import json
import time
import evpn
import os
import re


def df_cleaner(data_frame: pd.DataFrame) -> pd.DataFrame:
    print('Cleaning DataFrame...')
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame
    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is empty string
        if 'headline' in column:
            data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column

    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    print('DataFrame Cleaned...!')
    return data_frame


def set_na(text: str) -> str:
    # Remove extra spaces (assuming remove_extra_spaces is a custom function)
    text = remove_extra_spaces(text=text)
    pattern = r'^([^\w\s]+)$'  # Define a regex pattern to match all the conditions in a single expression
    text = re.sub(pattern=pattern, repl='N/A', string=text)  # Replace matches with "N/A" using re.sub
    return text


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def remove_diacritics(input_str):
    return ''.join(char for char in unicodedata.normalize('NFD', input_str) if not unicodedata.combining(char))


# def get_headline(li_tag) -> str:
#     headline = ' '.join(li_tag.xpath('./h3//text()'))
#     return headline if headline != '' else 'N/A'

def get_headline(li_tag) -> str:
    """Extracts the headline from the given li_tag."""
    headline = ' '.join(li_tag.xpath('./h3//text()')).strip().replace(' / ', ' | ')
    return headline if headline else 'N/A'


def get_date(li_tag) -> str:
    """Extracts the date from the given li_tag."""
    date = ' '.join(li_tag.xpath('./text()[normalize-space()]')).strip()
    return date if date else 'N/A'


# def get_external_url(li_tag) -> str:
#     """Extracts the "Read More" URL from the given li_tag."""
#     url_slug = ' '.join(li_tag.xpath('./p[contains(@class, "introduction")]/a/@href')).strip(':').strip()
#     # url = url_slug if url_slug.startswith('http') else 'https://www.fi.se' + url_slug
#     if url_slug.startswith('/'):
#         url = 'https://www.fi.se' + url_slug
#     else:
#         url = url_slug
#     return url if url else 'N/A'


def get_external_url(li_tag) -> str:
    """Extracts the "Read More" URL from the given li_tag.
    If the URL starts with 'ttp', it prepends 'http' to it."""
    url_slug = ' '.join(li_tag.xpath('./p[contains(@class, "introduction")]/a/@href')).strip(':').strip()
    if url_slug.startswith('/'):  # Relative URL case
        url = 'https://www.fi.se' + url_slug
    else:
        match = re.search(pattern=r'(?<=ttp)(.+)', string=url_slug)
        if match:
            url = 'http' + match.group(0)
        else:
            url = 'N/A'

    return url if url else 'N/A'


def get_source(li_tag) -> str:
    """Extracts the source text from the given li_tag."""
    source = ' '.join(li_tag.xpath('./p[contains(@class, "introduction")]/a//text()')).strip(':').strip()
    return source if source else 'N/A'


class FiSeSwedenSpider(scrapy.Spider):
    name = "fi_se_sweden"

    def __init__(self):
        self.start = time.time()
        super().__init__()
        print('Connecting to VPN (SWEDEN)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (SWEDEN)
        self.api.connect(country_id='203')  # SWEDEN country code for vpn
        time.sleep(10)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.final_data_list = list()  # List of data to make DataFrame then Excel

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}.xlsx"  # Filename with Scrape Date

        self.browsers = ["chrome110", "edge99", "safari15_5"]

        self.cookies = {
            'ASP.NET_SessionId': 'vrfoywftmn4bja3vputhu4da',
        }

        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://www.fi.se/en/our-registers/investor-alerts/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

    def start_requests(self) -> Iterable[Request]:

        query_params = {
            'q': '*',
            'page': '0',
            'take': '10',
            'lang': 'en'
        }

        # Sending request on an api which gives investor alerts html text in response json.
        url = 'https://www.fi.se/ajaxsearch/GetWarnedCompanyPagingResults?' + urlencode(query_params)
        yield scrapy.Request(url=url, method="GET", cookies=self.cookies, headers=self.headers, dont_filter=True, callback=self.parse,
                             cb_kwargs={'url': url, 'query_params': query_params}, meta={'impersonate': random.choice(self.browsers)})

    def parse(self, response, **kwargs):
        json_response = json.loads(response.text)
        # Scrape data from html after parsing it from json response
        parsed_tree = fromstring(json_response.get('hits', {}).get('row', 'N/A'))
        current_page_no = kwargs.get('query_params', {}).get('page', 'N/A')

        li_tags = parsed_tree.xpath('//li')
        for li_tag in li_tags:
            data_dict = dict()
            data_dict['url'] = 'https://www.fi.se/en/our-registers/investor-alerts/'

            data_dict['headline'] = get_headline(li_tag)
            data_dict['date'] = get_date(li_tag)
            data_dict['external_url'] = get_external_url(li_tag)
            data_dict['source'] = get_source(li_tag)
            # print(data_dict)
            self.final_data_list.append(data_dict)

        # Send Pagination request
        pagination_string = json_response.get('hits', {}).get('paging', 'N/A')
        if pagination_string and pagination_string != 'N/A':
            next_page_no = ' '.join(fromstring(pagination_string).xpath('//div[@id="paging"]/@data-page'))
            next_page_query_params = kwargs.get('query_params').copy()
            next_page_query_params['page'] = next_page_no
            url = f'https://www.fi.se/ajaxsearch/GetWarnedCompanyPagingResults?' + urlencode(next_page_query_params)
            print('Sending request on Page:', next_page_no)
            yield scrapy.Request(url=url, method="GET", cookies=self.cookies, headers=self.headers, dont_filter=True, callback=self.parse,
                                 cb_kwargs={'url': url, 'query_params': next_page_query_params}, meta={'impersonate': random.choice(self.browsers)})
        else:
            print('Pagination not found after page:', current_page_no)

    def close(self, reason):
        print('closing spider...')
        if self.final_data_list:
            try:
                print("Creating Native sheet...")
                data_df = pd.DataFrame(self.final_data_list)
                data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
                with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                    data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
                    data_df.to_excel(excel_writer=writer, index=False)
                print("Native Excel file Successfully created.")
            except Exception as e:
                print('Error while Generating Native Excel file:', e)
        else:
            print('Final-Data-List is empty, Hence not generating Excel File.')
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()
            print('VPN Connected!' if self.api.is_connected else 'VPN Disconnected!')
        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {FiSeSwedenSpider.name}'.split())
