import requests
import sqlite3
import json
import time
from telethon.sync import TelegramClient
from telethon import errors
from telethon.tl import types
import os


api_id = 12345678  # Замініть на свій айді
api_hash = 'abcdefgj'  # Замініть на свій хеш
phone_number = '+380991234567'  # Замініть на свій номер, до якого прив'язан телеграм акаунт
channel_url = 'https://t.me/+TwuUo6RvTj0xNjg6'  # Це реальний канал для демонстрації, де вже зібрані автомобілі. Замініть на свій канал.
channel_id = 2110193638     # Реальне айді каналу. Треба замінити на свій (ОБОВ'ЯЗКОВО треба мати права адміністратора по номеру з phone_number).


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
}


class Car:
    def __init__(self):
        self.name = ''
        self.price = 0
        self.vin = ''
        self.url = ''
        self.images = []

    @property
    def is_valid(self):
        required_values = ['name', 'price', 'vin', 'url', 'images']
        missing_values = [value for value in required_values if not getattr(self, value)]
        if missing_values:
            print(f"Відсутні значення: {', '.join(missing_values)}")
            return False
        return True

    def __str__(self):
        return f"Car(name={self.name}, price={self.price}, url={self.url}, images={self.images})"


def create_db():
    with sqlite3.connect('auto.db') as connect:
        connect.execute('PRAGMA foreign_keys = ON;')
        cursor = connect.cursor()

        cursor.execute('DROP TABLE IF EXISTS parsed_car;')
        cursor.execute("""
                    CREATE TABLE parsed_car (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        price INTEGER NOT NULL,
                        vin TEXT NOT NULL UNIQUE,
                        url TEXT NOT NULL
                    );
                """)

        cursor.execute("""
                    CREATE TABLE IF NOT EXISTS car (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        price INTEGER NOT NULL,
                        vin TEXT NOT NULL UNIQUE,
                        url TEXT NOT NULL
                    );
                """)

        cursor.execute('DROP TABLE IF EXISTS images;')
        cursor.execute("""
                    CREATE TABLE images (
                        id INTEGER PRIMARY KEY,
                        car_id INTEGER REFERENCES parsed_car(id) ON DELETE CASCADE,
                        image_url TEXT NOT NULL UNIQUE
                    );
                """)


def run(url: str):
    """
    Початок роботи
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    resp = session.get(url)
    if resp.status_code == 200:
        process_marks(resp.text, session=session)


def process_marks(data: str, session):
    """
    Збираємо айді марок
    """
    marks = json.loads(data)
    for mark in marks:
        name = mark.get('name')
        mark_id = mark.get('value')
        if name.lower() == 'toyota' and mark_id:
            url = f'https://auto.ria.com/api/categories/1/marks/{mark_id}/models'
            resp = session.get(url)
            if resp.status_code == 200:
                process_models(resp.text, dict(mark_id=mark_id), session)
                break


def process_models(data: str, context: dict, session):
    """
    Збираємо айді моделей автомобілів.
    """
    def process_countries(countries: list):
        """
        Збираємо айді країн для matched_country (країни, звідки експортується авто)
        :return: айді країни
        """
        for country in countries:
            country_name = country.get('name')
            country_id = country.get('value')
            if country_name.lower() == 'сша' and country_id:
                return country_id

    models = json.loads(data)
    for model in models:
        name = model.get('name')
        model_id = model.get('value')
        if name.lower() == 'sequoia' and model_id:
            country_url = 'https://auto.ria.com/api/countries'
            resp_country = session.get(country_url)
            if resp_country.status_code == 200:
                matched_country = process_countries(resp_country.json())
                url = f"https://auto.ria.com/api/search/auto?indexName=auto%2Corder_auto%2Cnewauto_search&category_id=1&marka_id%5B0%5D={context['mark_id']}&model_id%5B0%5D={model_id}&matched_country={matched_country}&abroad=2&countpage=100&page="
                resp = session.get(url + '0')
                if resp.status_code == 200:
                    process_prodlist(resp.text, dict(context, model_id=model_id, prods_url=url), session)
                    break


def process_prodlist(data: str, context: dict, session):
    """
    Збираємо айді автомобілів.
    """
    prods_json = json.loads(data)
    prods = prods_json.get('result', {}).get('search_result', {}).get('ids', [])
    for prod in prods:
        if len(prod) == 8:
            url = f'https://auto.ria.com/uk/bu/blocks/json/{prod[0:5]}/{prod[0:7]}/{prod}?lang_id=4'
            resp = session.get(url)
            if resp.status_code == 200:
                process_product(resp.text, dict(resp_url=resp.url))

    prods_count = prods_json.get('result', {}).get('search_result', {}).get('count')
    offset = context.get('offset', 0) + 50
    if prods_count and int(prods_count) > offset:
        next_page = context.get('page', 0) + 1
        next_url = context['prods_url'] + str(next_page)
        session.headers.update(HEADERS)
        resp_next = session.get(next_url)
        if resp_next.status_code == 200:
            time.sleep(2)
            process_prodlist(resp_next.text, dict(context, page=next_page, offset=offset), session)


def process_product(data: str, context: dict):
    """
    Обробляємо джсон автомобіля. Збираємо тут: марка автомобіля, ціна, посилання, фотографії.
    """
    def process_image(img_id: str):
        """
        Створює посилання на фотографію по айді, яке береться з джсона.
        """
        img_url = f'https://cdn4.riastatic.com/photosnew/auto/photo/toyota_sequoia__{img_id}fx.jpg'
        return img_url

    prod_json = json.loads(data)
    car = Car()
    car.name = f"{prod_json.get('title')} {prod_json.get('autoData', {}).get('year')}".strip()
    car.price = prod_json.get('USD')
    car.vin = prod_json.get('VIN')
    car.url = f'https://auto.ria.com/uk{prod_json.get("linkToView")}'

    images_id = prod_json.get('photoData', {}).get('all', [])
    for count, image_id in enumerate(images_id):
        if count > 9:
            break

        car.images.append(process_image(image_id))

    if car.is_valid:
        update_db(car)
    else:
        print(context['resp_url'], '___'*100, '\n')


def update_db(car: Car):
    """
    Додоє до БД автомобіль
    """
    print(f'Обробляю: {car}')
    with sqlite3.connect('auto.db') as conn:
        cur = conn.cursor()
        cur.execute('INSERT OR IGNORE INTO parsed_car (name, price, vin, url) VALUES (?, ?, ?, ?)', (car.name, car.price, car.vin, car.url))

        car_id = cur.execute('SELECT id FROM parsed_car WHERE vin = ?', (car.vin,)).fetchone()[0]
        images_data = [(car_id, img_url) for img_url in car.images]
        cur.executemany('INSERT OR IGNORE INTO images (car_id, image_url) VALUES (?, ?)', images_data)

        conn.commit()

        send_auto(car.vin, conn)


def send_auto(vin, conn):
    """
    Відправляти повідомлення на канал з автомобілем.
    """
    def check_car(vin_code, curs_check):
        return bool(curs_check.execute("SELECT 1 FROM car WHERE vin = ? LIMIT 1;", (vin_code,)).fetchone())

    def check_price(vin_code, conn_check):
        curs_check = conn_check.cursor()
        prices = curs_check.execute("""
            SELECT 
                car.price,
                parsed_car.price
            FROM car 
            JOIN parsed_car ON parsed_car.vin = car.vin 
            WHERE parsed_car.vin = ? AND parsed_car.price != car.price;
            """, (vin_code,)).fetchone()

        if prices:
            price = prices[0]
            parsed_price = prices[1]
            state = f"Ціна {'зменшилась' if price > parsed_price else 'збільшилась'}! Було: {price}$, стало {parsed_price}$."

            curs_check.execute("""
                UPDATE car 
                SET price = (SELECT parsed_car.price FROM parsed_car WHERE parsed_car.vin = ?)
                WHERE car.vin = ?;
            """, (vin_code, vin_code))
            conn_check.commit()

            return state
        return None

    with TelegramClient('TEST', api_id, api_hash) as client:
        curs = conn.cursor()

        auto = curs.execute("""
            SELECT name, price, url
            FROM parsed_car
            WHERE vin = ?;
        """, (vin,)).fetchone()

        if auto:
            imgs = curs.execute("""
                SELECT images.image_url, images.id
                FROM images
                JOIN parsed_car pc on pc.id = images.car_id
                WHERE pc.vin = ?;
            """, (vin,)).fetchall()

            downloaded_imgs = []
            for img_tuple in imgs:
                img_url = img_tuple[0]

                resp = requests.get(img_url)
                if resp.status_code == 500:
                    img_url = img_url.replace('fx', 'm')
                    curs.execute('''
                        UPDATE images
                        SET image_url = ?
                        WHERE id = ?;''', (img_url, imgs[1]))

                if resp.status_code == 200:
                    os.makedirs('images', exist_ok=True)
                    img_name = os.path.basename(img_url)
                    img_path = f"images/{img_name}"
                    if not os.path.exists(img_path):
                        with open(img_path, 'wb') as file:
                            file.write(resp.content)

                    downloaded_imgs.append(img_path)

            is_exist = check_car(vin, curs_check=curs)
            if is_exist:
                print('Перевіряю ціну...')
                result = check_price(vin, conn)
                if not result:
                    print('Ціна не змінилась.\n')
                else:
                    url = curs.execute("SELECT url FROM car WHERE vin = ?", (vin,)).fetchone()
                    channel_entity = client.get_entity(types.PeerChannel(channel_id=channel_id))
                    post = client.get_messages(channel_entity, search=url[0])
                    try:
                        print(f'{result}\t {time.ctime()}')
                        client.send_message(channel_id, message=result, reply_to=post[0].id)
                        time.sleep(20)
                    except errors.FloodWaitError as e:
                        print(f'Треба взяти відпочинок на {e.seconds} секунд. Обмеження телеграма! Не забудьте перезапустити програму.')

            if downloaded_imgs and not is_exist:
                print('Додаю новий автомобіль до бази...')
                message = f"""<a href="{auto[2]}">{auto[0]}</a>\n💵 {'{:,}'.format(auto[1]).replace(',', ' ')} $"""
                try:
                    client.send_file(channel_id, file=downloaded_imgs, caption=message, parse_mode='html', link_preview=False)
                    print(f'Відправлено на канал!\t<{time.ctime()}>\n')
                    time.sleep(25)
                except errors.FloodWaitError as e:
                    print(f'Треба взяти відпочинок на {e.seconds} секунд. Обмеження телеграма! Не забудьте перезапустити програму.')

                curs.execute("""
                    INSERT INTO car (name, price, vin, url) 
                    SELECT name, price, vin, url
                    FROM parsed_car
                    WHERE parsed_car.vin = ?;
                    """, (vin,))

                conn.commit()

            # Якщо треба видаляти фотографії після їх завантаження, то треба видалити "#" у двох рядках нижче.
            # for img in downloaded_imgs:
            # os.remove(img)


def compare_tables():
    """
    Перевіряє, яких машин немає в parsed_car, але які є в car.
    Якщо машини в parsed_car немає, то відправити повідомлення на канал (відповіддю),
    що машина продана, а потім видалити її з car.
    :return:
    """
    def find_and_delete(car):
        with TelegramClient('TEST', api_id, api_hash) as client:
            channel_entity = client.get_entity(channel_id)
            post = client.get_messages(channel_entity, search=car[2])
            if post:
                message = f"Автомобіль {car[0]} (VIN: {car[3]}) був проданий. Його остання ціна: {car[1]}$."
                client.send_message(channel_id, message=message)
                client.delete_messages(channel_id, post[0].id)
                return True
            return False

    with sqlite3.connect('auto.db') as conn:
        cursor = conn.cursor()

        parsed_car_vins = {vin for vin, in cursor.execute("SELECT vin FROM parsed_car").fetchall()}
        car_vins = {vin for vin, in cursor.execute("SELECT vin FROM car").fetchall()}

        missing_vins = car_vins - parsed_car_vins
        for vin in missing_vins:
            car_info = cursor.execute("SELECT name, price, url, vin FROM car WHERE vin = ?", (vin,)).fetchone()
            if find_and_delete(car_info):
                print(f"Автомобіль {car_info} продан! Видаляю з бази...")
                cursor.execute("DELETE FROM car WHERE vin = ?", (vin,))
                conn.commit()


if __name__ == "__main__":
    create_db()
    URL = 'https://auto.ria.com/api/categories/1/marks'     # "1" — легкові автомобілі
    run(URL)
    compare_tables()
    print('\nЛягаю спати...')
