import asyncio
import uuid
from datetime import datetime

import aiohttp
import time
import csv

REPORTS_ENDPOINT = 'https://analytics.maximum-auto.ru/vacancy-test/api/v0.1'
header = {"Authorization": "Bearer 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"}

waiting_dict = {}


async def create_report(session):

    """ Функция для создания запроса на отчет"""

    report_id = uuid.UUID()
    data = {'id': report_id}
    async with session.post(REPORTS_ENDPOINT, headers=header, data=data) as response:
        if response.status == 201:
            created_at = time.time()
            waiting_dict['report_id'] = created_at


async def get_report(session, report_id):

    """ Функция для запроса получения отчета каждую секунду"""

    url = f'{REPORTS_ENDPOINT}{report_id}'
    data = {'id': report_id}
    async with session.get(url, headers=header, data=data) as response:
        if response.status == 200:
            response_data = await response.json()
            del waiting_dict[report_id]
            return response_data['value']
    await asyncio.sleep(1)


async def write_to_csv(report_id, created_at, value):

    """ Функция для создания записи результатов в файл"""

    with open('results.csv', mode='a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        report_sent_timestamp = int(datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ').timestamp())
        writer.writerow([report_sent_timestamp, value])


async def create_reports():

    """ Функция, которая спит минуту между созданием отчетов """

    async with aiohttp.ClientSession() as session:
        while True:
            await create_report(session)
            await asyncio.sleep(60)


async def check_reports():

    """ Функция, которая обходит словарь с отчетами, забирает готовый и пишет в файл"""

    async with aiohttp.ClientSession() as session:
        while True:
            for report, creation_time in waiting_dict.items():
                value = await get_report(session, report)
                await write_to_csv(report, creation_time, value)


async def main():
    task1 = asyncio.create_task(create_reports())
    task2 = asyncio.create_task(check_reports())
    await task1
    await task2

if __name__ == '__main__':
    asyncio.run(main())
