import WebScrapper
import DBhandler

MAX_PAGES = 50

def main():

    driver = WebScrapper.get_driver()
    conn = DBhandler.create_database()

    for page_num in range(1, MAX_PAGES + 1):
        driver.get(f"https://career.habr.com/vacancies?page={page_num}type=all")
        vacancies = WebScrapper.parsePage(driver)
        DBhandler.insert_vacancies(conn, vacancies)


if __name__ == "__main__":
    main()