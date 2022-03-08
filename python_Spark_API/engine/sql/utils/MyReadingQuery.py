class ReadingQuery:
    """
    Класс, предназначенный для чтения SQL-запроса из файла
    """
    def __init__(self, path):
        # Передаем в экземляр класса путь к файлу с SQL-запросом
        self.path = path

    def read_query_from_file(self):
        # Функция читает SQL-запрос из файла и возвращает его в виде строки
        # Строка-накопитель:
        result = ""

        try:
            with open(self.path, "r", encoding="utf-8") as file_query:
                for line in file_query:
                    result = " ".join([result, line])

        except FileNotFoundError:
            print("FileNotFoundError!")

        finally:
            return result