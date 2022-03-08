class SqlQuery:
    """
    Класс SqlQuery, позволяет заменять параметры в тексте SQL-запроса на переданные значения
    """
    def __init__(self, text):
        # Передаем в экземпляр класса текст SQL-запроса
        self.text = text

    def replace_text(self, old_text, new_text):
        # Функция заменяет параметры в тексте SQL-запроса на переданные значения
        result_text = self.text.replace(old_text, new_text)
        return SqlQuery(result_text)