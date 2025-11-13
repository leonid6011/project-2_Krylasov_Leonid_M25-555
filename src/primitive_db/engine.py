from prompt import string


def print_help() -> None:
    """Выводит список доступных комнад."""
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")

def welcome() -> None:
    """Приветствие и простой цикл ввода команд через prompt."""
    print("Первая попытка запустить проект!")
    print()
    print("***")
    print_help()

    while True:
        command = string("Введите команду: ").strip().lower()

        if command == "exit":
            print("Выход из программы. Пока!")
            break
        elif command == "help" or command == "":
            #если пусть или help то выводим подсказку ещё раз
            print_help()
        else:
            print(f"Неизвестная команда: {command}")
            print("Введите 'help', чтобы увидеть список команд.")