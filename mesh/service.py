from asynch.cursors import DictCursor

from settings import clickhouse_holder
from utils.utils import ResponseObject


class MeshService:
    class CategoryConfig:
        def __init__(self, r_name, table):
            self.r_name = r_name
            self.table = table

    schema = 'mesh'
    categories_config_map = {
        'visits': CategoryConfig('Визиты', 'sessions'),
        'uniq_users': CategoryConfig('Уникальные пользователи', 'uniqusers'),
        'devices': CategoryConfig('Тип устройства', 'devices'),
        'os': CategoryConfig('Операционная система устройства', 'os'),
        'ages': CategoryConfig('Возраст', 'ages'),
        'geography': CategoryConfig('География', 'ao')
    }
    filters_map = {
        'segment': 'Сегмент',
        'from': 'Начало периода',
        'to': 'Конец периода',
        'product': 'Продукт',
        'type': 'Тип'
    }

    @classmethod
    async def get_date_ranges(cls, category):
        if category not in cls.categories_config_map:
            return ResponseObject(meta=f'Для категории {category} не задана таблица')
        sql = f'select  min(from_dt) as min,  max(from_dt) as max from {cls.schema}.{cls.categories_config_map[category].table}'
        async with await clickhouse_holder.get_connection() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sql)
                return ResponseObject(data=await cursor.fetchone())

    @classmethod
    async def get_aos_and_districts(cls):
        source = f'{cls.schema}.{cls.categories_config_map['geography'].table}'
        sql = (f'select distinct AO, District from {source} where '
               f'from_dt  = (select max(from_dt) from {source}) order by AO')
        async with await clickhouse_holder.get_connection() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sql)
                data = await cursor.fetchall()
                aos_districts = {k['AO']: list() for k in data}
                for row in data:
                    aos_districts[row['AO']].append(row['District'])
                return ResponseObject(data=[{
                    'ao_name': k,
                    'districts': v
                } for k, v in aos_districts.items()])

    @classmethod
    async def get_ages_data(cls):
        source = f'{cls.schema}.{cls.categories_config_map['ages'].table}'
        sql = f'select * from {source} where from_dt  = (select max(from_dt) from {source}) order by section, age_range'
        async with await clickhouse_holder.get_connection() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(sql)
                data = await cursor.fetchall()
                response = ResponseObject(
                    data=list(),
                    meta={
                        'section_values': set(),
                        'age_range_values': set()
                    }
                )
                response.meta['date'] = data[0]['from_dt']
                for row in data:
                    response.meta['section_values'].add(row['section'])
                    response.meta['age_range_values'].add(row['age_range'])
                    response.data.append({k: v for k, v in row.items() if k != 'from_dt'})
                response.meta['age_range_values'] = sorted(response.meta['age_range_values'])
                return response
