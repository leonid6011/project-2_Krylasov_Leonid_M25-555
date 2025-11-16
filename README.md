# Примитивная база данных
Учебный консольный проект: простая база данных на Python с поддержкой создания таблиц, операций CRUD и декораторов для обработки ошибок, подверждения дейтсвий и логгирования времени.
Проект выполнен в рамках обучения по программе МИФИ х Яндекс Практикум.
## Возможности
### Общие команды
### Работа с таблицами
- `<command> create_table <имя_таблицы> <столбец1:тип> ..` - создать таблицу
- `<command> list_tables` - показать список всех таблиц
- `<command> drop_table <имя_таблицы>` - удалить таблицу

[![asciicast](https://asciinema.org/a/IZDc5g6Mu6yX7mzhvvGWF7jvO.svg)](https://asciinema.org/a/IZDc5g6Mu6yX7mzhvvGWF7jvO)
### CRUD-операции
- `<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...)` - создать запись.
- `<command> select from <имя_таблицы> where <столбец> = <значение>` - прочитать записи по условию.
- `<command> select from <имя_таблицы>` - прочитать все записи.
- `<command> update <имя_таблицы> set <столбец1> = <новое_значение1> where <столбец_условия> = <значение_условия>` - обновить запись.
- `<command> delete from <имя_таблицы> where <столбец> = <значение>` - удалить запись.
- `<command> info <имя_таблицы>` - вывести информацию о таблице.

[![asciicast](https://asciinema.org/a/eE5pOAPlIlFJq4uwEVb3iKvlj.svg)](https://asciinema.org/a/eE5pOAPlIlFJq4uwEVb3iKvlj)
### Обработка ошибок, подтверждение действий
[![asciicast](https://asciinema.org/a/evUdeKZyyhGzf9qyBZvxfVDwQ.svg)](https://asciinema.org/a/evUdeKZyyhGzf9qyBZvxfVDwQ)
## Установка
1. Клонируйте репозиторий:
```bash
git clone https://github.com/leonid6011/project-2_Krylasov_Leonid_M25-555
cd project-2_Krylasov_Leonid_M25-555
```
2. Установите зависимости
```bash
poetry install
```
или
```bash
make install
```
3. Запуск проекта
```bash
poetry run database
```
или
```bash
make project
```
или
```bash
database
```
## Автор
Леонид Крыласов
