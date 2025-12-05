import sqlite3
import re
from datetime import datetime, date


def create_database():
    """Создает базу данных и таблицу"""
    conn = sqlite3.connect('habr_vacancies.db')
    cursor = conn.cursor()

    # Удаляем старую таблицу если существует
    cursor.execute('DROP TABLE IF EXISTS vacancies')

    # Создаем таблицу с улучшенной структурой
    cursor.execute('''
                   CREATE TABLE vacancies
                   (
                       id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                       date_posted           TEXT,    -- Оригинальная дата, например "3 декабря"
                       date_posted_timestamp DATE,    -- Дата в формате timestamp (2025-12-03)
                       company_name          TEXT,
                       company_rating        REAL,
                       vacancy_title         TEXT,
                       location              TEXT,
                       employment_type       TEXT,
                       remote_option         BOOLEAN,

                       -- Поля для зарплаты
                       salary_text           TEXT,
                       salary_min            INTEGER,
                       salary_max            INTEGER,
                       salary_currency       TEXT,
                       is_exact_salary       BOOLEAN, -- True = точная зарплата, False = похожие специалисты

                       skills                TEXT,
                       scraped_date          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                   )
                   ''')

    # Создаем индексы для быстрого поиска
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company ON vacancies(company_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON vacancies(vacancy_title)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_salary ON vacancies(salary_min, salary_max)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON vacancies(date_posted_timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_exact_salary ON vacancies(is_exact_salary)')

    conn.commit()
    return conn


def parse_date_to_timestamp(date_text):
    """
    Парсит дату в формате "3 декабря" в timestamp 2025-12-03.
    Если дата не распознается, возвращает текущую дату.
    """
    if not date_text or not isinstance(date_text, str):
        return datetime.now().strftime('%Y-%m-%d')

    # Очищаем текст
    date_text = date_text.strip().lower()

    # Словарь для месяцев
    month_dict = {
        'января': 1, 'янв': 1,
        'февраля': 2, 'фев': 2,
        'марта': 3, 'мар': 3,
        'апреля': 4, 'апр': 4,
        'мая': 5, 'май': 5,
        'июня': 6, 'июн': 6,
        'июля': 7, 'июл': 7,
        'августа': 8, 'авг': 8,
        'сентября': 9, 'сен': 9,
        'октября': 10, 'окт': 10,
        'ноября': 11, 'ноя': 11,
        'декабря': 12, 'дек': 12,
    }

    # Паттерны для разных форматов дат
    patterns = [
        # "3 декабря"
        r'(\d{1,2})\s+(\w+)',
    ]

    current_year = datetime.now().year

    for pattern in patterns:
        match = re.search(pattern, date_text)
        if match:
            # Извлекаем день и месяц
            day_str = match.group(1)
            month_str = match.group(2)

            try:
                day = int(day_str)

                # Ищем месяц в словаре
                month = None
                for key, value in month_dict.items():
                    if key in month_str:
                        month = value
                        break

                if month:
                    # Проверяем валидность даты
                    try:
                        # Создаем дату с текущим годом
                        parsed_date = date(current_year, month, day)

                        # Если дата в будущем (например, сегодня 10 декабря, а дата 3 декабря),
                        # значит это прошлый год
                        if parsed_date > datetime.now().date():
                            parsed_date = date(current_year - 1, month, day)

                        return parsed_date.strftime('%Y-%m-%d')
                    except ValueError:
                        # Некорректная дата (например, 32 декабря)
                        return datetime.now().strftime('%Y-%m-%d')
            except ValueError:
                pass

    # Если не удалось распарсить, возвращаем текущую дату
    return datetime.now().strftime('%Y-%m-%d')


def parse_salary(salary_text):
    """Парсит текст зарплаты в числовые значения"""
    if not salary_text or salary_text.strip() == '':
        return None, None, None, salary_text, False

    # Сохраняем оригинальный текст
    original_text = salary_text

    # 1. Проверяем, есть ли "Похожие специалисты получают"
    # Это имеет приоритет - извлекаем зарплату даже если есть "не указана"
    similar_pattern = r'Похожие специалисты получают\s*([\d\s]+)\s*[-–]\s*([\d\s]+)'
    similar_match = re.search(similar_pattern, salary_text, re.IGNORECASE)

    if similar_match:
        try:
            # Извлекаем числа
            salary_min = int(similar_match.group(1).replace(' ', '').replace(',', ''))
            salary_max = int(similar_match.group(2).replace(' ', '').replace(',', ''))
            currency = 'RUB'  # Предполагаем рубли для похожих специалистов на habr

            # Если в тексте есть "не указана", но есть "похожие специалисты"
            # все равно возвращаем зарплату, но с флагом is_exact=False
            return salary_min, salary_max, currency, original_text, False
        except ValueError as e:
            print(f"Ошибка при парсинге похожих зарплат: {e}")

    # 2. Если нет "похожих специалистов", проверяем обычную логику
    if 'не указана' in salary_text.lower():
        return None, None, None, salary_text, False

    # Очищаем текст для парсинга
    salary_text = salary_text.replace(' ', '').replace(',', '.')

    # Пытаемся извлечь числа из текста
    patterns = [
        # "от X до Y валюта"
        r'от(\d+[\d]*)до(\d+[\d]*)([₽$€]|руб|USD|EUR)',
        # "X - Y валюта"
        r'(\d+[\d]*)[-–](\d+[\d]*)([₽$€]|руб|USD|EUR)',
        # "до X валюта"
        r'до(\d+[\d]*)([₽$€]|руб|USD|EUR)',
        # "от X валюта"
        r'от(\d+[\d]*)([₽$€]|руб|USD|EUR)',
        # "X валюта" (фиксированная)
        r'(\d+[\d]*)([₽$€]|руб|USD|EUR)',
        # Просто числа без явной валюты (предполагаем рубли)
        r'от(\d+[\d]*)до(\d+[\d]*)',
        r'(\d+[\d]*)[-–](\d+[\d]*)',
        r'до(\d+[\d]*)',
        r'от(\d+[\d]*)',
        r'(\d+[\d]*)'
    ]

    salary_min = None
    salary_max = None
    currency = None
    is_exact = True  # По умолчанию считаем точной зарплатой

    for pattern in patterns:
        match = re.search(pattern, salary_text, re.IGNORECASE)
        if match:
            groups = match.groups()

            if len(groups) == 3:  # "от X до Y валюта" или "X - Y валюта"
                try:
                    salary_min = int(groups[0])
                    salary_max = int(groups[1])
                    currency = parse_currency(groups[2])
                    break
                except:
                    continue

            elif len(groups) == 2:  # "от X валюта" или "до X валюта" или "X валюта"
                try:
                    if pattern.startswith('до'):  # "до X"
                        salary_max = int(groups[0])
                        salary_min = None
                    elif pattern.startswith('от'):  # "от X"
                        salary_min = int(groups[0])
                        salary_max = None
                    else:  # "X валюта"
                        salary_min = int(groups[0])
                        salary_max = salary_min

                    currency = parse_currency(groups[1])
                    break
                except:
                    continue

            elif len(groups) == 1:  # Просто число
                try:
                    if pattern.startswith('до'):
                        salary_max = int(groups[0])
                    elif pattern.startswith('от'):
                        salary_min = int(groups[0])
                    else:
                        # Если просто число, предполагаем что это фиксированная зарплата
                        salary_min = int(groups[0])
                        salary_max = salary_min

                    # Для чисел без явной валюты проверяем контекст
                    if '$' in original_text:
                        currency = 'USD'
                    elif '€' in original_text or 'евро' in original_text.lower():
                        currency = 'EUR'
                    else:
                        currency = 'RUB'  # По умолчанию рубли
                    break
                except:
                    continue

    return salary_min, salary_max, currency, original_text, is_exact


def parse_currency(currency_text):
    """Определяет валюту по тексту"""
    if not currency_text:
        return 'RUB'

    currency_text = currency_text.upper()

    if '₽' in currency_text or 'RUB' in currency_text or 'РУБ' in currency_text:
        return 'RUB'
    elif '$' in currency_text or 'USD' in currency_text:
        return 'USD'
    elif '€' in currency_text or 'EUR' in currency_text or 'ЕВРО' in currency_text:
        return 'EUR'
    else:
        return 'RUB'  # По умолчанию рубли


def parse_company_info(company_text):
    """Парсит название компании и рейтинг"""
    if not company_text:
        return None, None

    lines = company_text.strip().split('\n')
    company_name = lines[0].strip()
    rating = None

    # Пытаемся извлечь рейтинг
    if len(lines) > 1:
        try:
            rating = float(lines[1].strip())
        except:
            # Ищем рейтинг в тексте
            match = re.search(r'(\d+\.\d+)', company_text)
            if match:
                try:
                    rating = float(match.group(1))
                except:
                    pass

    return company_name, rating


def parse_location_employment(location_text):
    """Парсит местоположение и тип занятости"""
    if not location_text:
        return None, None, False

    location = location_text
    employment_type = None
    remote_option = 'удаленно' in location_text.lower() or 'удалённо' in location_text.lower()

    # Определяем тип занятости
    if 'Полный рабочий день' in location_text:
        employment_type = 'Полная'
    elif 'Неполный рабочий день' in location_text:
        employment_type = 'Частичная'
    elif 'Проектная работа' in location_text:
        employment_type = 'Проектная'
    elif 'Стажировка' in location_text:
        employment_type = 'Стажировка'

    return location, employment_type, remote_option


def insert_vacancies(conn, data):
    """Вставляет данные в базу"""
    cursor = conn.cursor()

    for item in data:
        date_posted_original = item[0]
        company_text = item[1]
        vacancy_title = item[2]
        location_text = item[3]
        salary_text = item[4]
        skills = item[5]

        # Парсим дату в timestamp
        date_posted_timestamp = parse_date_to_timestamp(date_posted_original)

        # Парсим остальные данные
        company_name, company_rating = parse_company_info(company_text)
        location, employment_type, remote_option = parse_location_employment(location_text)
        salary_min, salary_max, salary_currency, parsed_salary_text, is_exact_salary = parse_salary(salary_text)

        # Вставляем в базу
        cursor.execute('''
                       INSERT INTO vacancies
                       (date_posted, date_posted_timestamp, company_name, company_rating, vacancy_title, location,
                        employment_type, remote_option, salary_text, salary_min, salary_max, salary_currency,
                        is_exact_salary, skills)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ''', (
                           date_posted_original, date_posted_timestamp, company_name, company_rating,
                           vacancy_title, location, employment_type, remote_option, parsed_salary_text,
                           salary_min, salary_max, salary_currency, is_exact_salary, skills
                       ))

    conn.commit()
    print(f"✅ Добавлено {len(data)} записей в базу данных")


def export_to_csv(conn, filename='vacancies.csv'):
    """Экспорт данных в CSV"""
    import csv

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM vacancies")

    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Заголовки
        writer.writerow([description[0] for description in cursor.description])

        # Данные
        writer.writerows(cursor.fetchall())

    print(f"📄 Данные экспортированы в {filename}")


