import json

import requests
import uuid
import os

import DBhandler
import configHandler

CERT_PATH = os.path.join(os.path.dirname(__file__), configHandler.cert_path)


def get_token(auth_token, scope='GIGACHAT_API_PERS'):
    # Создадим идентификатор UUID (36 знаков)
    rq_uid = str(uuid.uuid4())
    # API URL
    url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    # Заголовки
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'RqUID': rq_uid,
        'Authorization': f'Basic {auth_token}'
    }

    # Тело запроса
    payload = {
        'scope': scope
    }

    try:
        # Делаем POST запрос с отключенной SSL верификацией
        # (можно скачать сертификаты Минцифры, тогда отключать проверку не надо)
        response = requests.post(url, headers=headers, data=payload, verify=CERT_PATH)
        return response
    except requests.RequestException as e:
        print(f"Ошибка: {str(e)}")
        return -1


def validate_skills_for_vacancy(vacancy_title, skills):
    response = get_token(configHandler.auth)
    if response != 1:
        print(response.text)
        giga_token = response.json()['access_token']

    url = configHandler.base_url + "/api/v1/chat/completions"

    prompt = f"""Проанализируй следующую вакансию и список навыков.
            Задача: определить, насколько список навыков соответствует должности.
    
            Должность: {vacancy_title}
    
            Список навыков из описания вакансии:
            {skills}
    
            Проанализируй и ответь в формате JSON:
            {{
                "match_score": число от 0 до 100 (процент соответствия),
                "is_relevant": true/false (соответствует ли должности),
                "missing_skills": ["список важных навыков, которых не хватает"],
                "redundant_skills": ["список навыков, не относящихся к должности"],
                "analysis": "краткий анализ соответствия (1-2 предложения)",
                "recommendations": ["рекомендации по улучшению описания навыков"]
            }}
    
            Важные критерии:
            1. Технические навыки должны соответствовать должности
            2. Soft skills должны быть релевантны
            3. Уровень навыков (junior/middle/senior) должен соответствовать позиции
            4. Учитывай современные требования к подобным позициям
            """
    payload = json.dumps({
        "model": "GigaChat:1.0.26.20",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    })

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {giga_token}'
    }

    response = requests.post(url, headers=headers, data=payload, verify=CERT_PATH)
    return response.json()


def gigachat_analyse(vacancies):
    vacancies_after_analyse = list()
    for vacancy in vacancies:
        # Получаем ответ от GigaChat
        response = validate_skills_for_vacancy(vacancy['company_name'], vacancy['skills'])

        # Для отладки: посмотрим структуру ответа
        print("Полный ответ GigaChat:")
        print(response)
        print("\n" + "=" * 80 + "\n")

        # Парсим JSON из текста ответа
        try:
            # Получаем текст ответа
            content = response['choices'][0]['message']['content']

            # Находим JSON в тексте (между ```json и ```)
            import json
            import re

            # Ищем JSON в ответе
            json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)

            if json_match:
                json_text = json_match.group(1)
                # Парсим JSON
                analysis_data = json.loads(json_text)
            else:
                # Если JSON не найден, пытаемся найти любые {} в тексте
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    json_text = json_match.group(0)
                    analysis_data = json.loads(json_text)
                else:
                    print(f"Не удалось найти JSON в ответе: {content}")
                    continue

            # Создаем результат с данными из анализа
            result = vacancy.copy()
            result['match_score'] = analysis_data.get('match_score')
            result['is_relevant'] = analysis_data.get('is_relevant')
            result['missing_skills'] = analysis_data.get('missing_skills', [])
            result['redundant_skills'] = analysis_data.get('redundant_skills', [])
            result['analysis'] = analysis_data.get('analysis', '')
            result['recommendations'] = analysis_data.get('recommendations', [])

            vacancies_after_analyse.append(result)

        except json.JSONDecodeError as e:
            print(f"Ошибка при парсинге JSON: {e}")
            print(f"Текст для парсинга: {json_text}")
            continue
        except KeyError as e:
            print(f"Ошибка ключа в ответе GigaChat: {e}")
            print(f"Структура ответа: {response.keys() if isinstance(response, dict) else response}")
            continue
        except Exception as e:
            print(f"Общая ошибка: {e}")
            continue

    return vacancies_after_analyse
