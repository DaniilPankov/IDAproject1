from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    prefs = {
        'profile.managed_default_content_settings.images': 2,
        'profile.managed_default_content_settings.stylesheets': 2,
        'profile.managed_default_content_settings.javascript': 2,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    return webdriver.Chrome(options=chrome_options)


def parsePage(driver):
    vacancy_blocks = driver.find_elements(By.CLASS_NAME, "vacancy-card__inner")

    vacancies = list()

    def getOrNone(code):
        try:
            return code()
        except:
            return None

    for block in vacancy_blocks:
        date = getOrNone(
            lambda: block.find_element(By.CLASS_NAME, "vacancy-card__date").text.strip()
        )
        company = getOrNone(
            lambda: block.find_element(By.CLASS_NAME, "vacancy-card__company").text.strip()
        )
        title = getOrNone(
            lambda: block.find_element(By.CLASS_NAME, "vacancy-card__title").text.strip()
        )
        meta = getOrNone(
            lambda: block.find_element(By.CLASS_NAME, "vacancy-card__meta").text.strip()
        )
        salary = getOrNone(
            lambda: block.find_element(By.CLASS_NAME, "vacancy-card__salary").text.strip()
        )
        skills = getOrNone(
            lambda: block.find_element(By.CLASS_NAME, "vacancy-card__skills").text.strip()
        )

        vacancies.append((date, company, title, meta, salary, skills))

    return vacancies

