import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor

# Список штатов с их координатами
states_coordinates = {
    'Alabama': (32.806671, -86.791130), 'Alaska': (61.370716, -152.404419), 'Arizona': (33.729759, -111.431221),
    'Arkansas': (34.969704, -92.373123), 'California': (36.778259, -119.417931), 'Colorado': (39.550051, -105.782067),
    'Connecticut': (41.603221, -73.087749), 'Delaware': (38.910832, -75.527670), 'Florida': (27.994402, -81.760254),
    'Georgia': (32.165622, -82.900075), 'Hawaii': (19.896766, -155.582779), 'Idaho': (44.068202, -114.742041),
    'Illinois': (40.633125, -89.398528), 'Indiana': (40.267194, -86.134902), 'Iowa': (41.878003, -93.097702),
    'Kansas': (39.011902, -98.484246), 'Kentucky': (37.839333, -84.270018), 'Louisiana': (30.984299, -91.962333),
    'Maine': (45.253783, -69.445469), 'Maryland': (39.045755, -76.641271), 'Massachusetts': (42.407211, -71.382437),
    'Michigan': (44.314844, -85.602364), 'Minnesota': (46.729553, -94.685900), 'Mississippi': (32.354668, -89.398528),
    'Missouri': (37.964253, -91.831833), 'Montana': (46.879682, -110.362566), 'Nebraska': (41.492537, -99.901813),
    'Nevada': (38.802610, -116.419389), 'New Hampshire': (43.193852, -71.572395), 'New Jersey': (40.058324, -74.405661),
    'New Mexico': (34.519940, -105.870090), 'New York': (43.299428, -74.217933),
    'North Carolina': (35.759573, -79.019300),
    'North Dakota': (47.551493, -101.002012), 'Ohio': (40.417287, -82.907123), 'Oklahoma': (35.007752, -97.092877),
    'Oregon': (43.804133, -120.554201), 'Pennsylvania': (41.203322, -77.194525),
    'Rhode Island': (41.580095, -71.477429),
    'South Carolina': (33.836081, -81.163725), 'South Dakota': (43.969515, -99.901813),
    'Tennessee': (35.517491, -86.580447),
    'Texas': (31.968599, -99.901813), 'Utah': (39.320980, -111.093731), 'Vermont': (44.558803, -72.577841),
    'Virginia': (37.431573, -78.656894), 'Washington': (47.751076, -120.740135),
    'West Virginia': (38.597626, -80.454903),
    'Wisconsin': (43.784440, -88.787868), 'Wyoming': (43.075968, -107.290284)
}

url = "https://nodig.com/listing/get-public-listings"


async def get_companies_in_state(state_name, lat, lng, session, executor):
    params = {
        'geocode_address': f'{state_name}, USA',
        'rating': '',
        'radius': 300,
        'initial_fetch': 0,
        'restrict_fetch': 1,
        'user_latitude': '',
        'user_longitude': '',
        'map_latitude': '',
        'map_longitude': '',
        'clicked_on_maps': 0,
        'lat_for_center': lat,
        'lng_for_center': lng,
        'service_id': ''
    }

    async with session.get(url, params=params) as response:
        if response.status == 200:
            data = await response.text()
            data = json.loads(data)
            companies = data.get('data', {})

            rows = []
            for company_data in companies.values():
                rows.append(await get_company_services(company_data, session, executor))
            return rows
        else:
            print(f"Error getting companies in {state_name}: {response.status}")
            return []


async def get_company_services(company_data, session, executor):
    company_name = company_data.get('name', 'N/A')
    website = company_data.get('website', 'N/A')
    city = company_data.get('city', 'N/A')
    state = company_data.get('state', 'N/A')
    rating = company_data.get('google_review_count', 'N/A')
    slug = company_data.get('slug', 'N/A')
    services = []

    for service in company_data.get('services', []):
        service_data = service.get('service', None)
        if isinstance(service_data, dict):
            service_name = service_data.get('name', 'N/A')
            services.append(service_name)
        elif service_data:
            service_name = service_data
            services.append(service_name)

    if not services and city and state:
        company_url = f"https://nodig.com/{state.replace(' ', '-').lower()}/{city.replace(' ', '-').lower()}/trenchless-sewer-repair/{slug}"

        print(f"Send request to company page: {company_name} ({company_url})")

        try:
            async with session.get(company_url) as company_response:
                if company_response.status == 200:
                    html = await company_response.text()
                    services = await asyncio.get_event_loop().run_in_executor(executor, parse_services, html)
                    if not services:
                        services = ['N/A']
                else:
                    services = ['N/A']
        except Exception as e:
            print(f"Error fetching company {company_name}: {e}")
            services = ['N/A']
    return [company_name, website, city, state, rating, ', '.join(services)]


def parse_services(html):
    soup = BeautifulSoup(html, 'html.parser')
    service_elements = soup.select('.card-body .list-categories .row .col-lg-3 h6')
    return [service.text.strip() for service in service_elements]


async def main():
    with ThreadPoolExecutor() as executor:
        async with aiohttp.ClientSession() as session:
            tasks = [
                get_companies_in_state(state, lat, lng, session, executor)
                for state, (lat, lng) in states_coordinates.items()
            ]

            results = await asyncio.gather(*tasks)

    all_rows = [row for state_results in results for row in state_results]

    all_rows.sort(key=lambda x: x[3])

    headers = ['Company Name', 'Website', 'City', 'State', 'Rating', 'Services']
    df = pd.DataFrame(all_rows, columns=headers)
    df.to_excel('sorted_companies.xlsx', index=False)


asyncio.run(main())