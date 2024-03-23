import json
from pymongo import MongoClient

def check_and_load_data(quotes_file, authors_file, db_name, host):
    client = MongoClient(host)
    
    if db_name in client.list_database_names():
        return False

    db = client[db_name]

    with open(quotes_file, 'r', encoding='utf-8') as f:
        quotes_data = json.load(f)

    quotes_collection = db['quotes']
    quotes_collection.insert_many(quotes_data)

    with open(authors_file, 'r', encoding='utf-8') as f:
        authors_data = json.load(f)

    authors_collection = db['authors']
    authors_collection.insert_many(authors_data)

    client.close()

    return True


if __name__ == '__main__':
    db_created = check_and_load_data('quotes.json', 'authors.json', 'hw_9', 'mongodb://localhost:27017/')

    if db_created:
        print("База даних успішно створена.")
    else:
        print("База даних вже існує.")