import sqlite3
import csv
from typing import List


def create_database():
    """Tworzy strukturę bazy danych"""
    conn = sqlite3.connect('movies.db')
    cursor = conn.cursor()

    # Tabela movies
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            movieId INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            genres TEXT
        )
    ''')

    # Tabela links
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS links (
            movieId INTEGER PRIMARY KEY,
            imdbId TEXT,
            tmdbId TEXT,
            FOREIGN KEY (movieId) REFERENCES movies (movieId)
        )
    ''')

    # Tabela ratings
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ratings (
            userId INTEGER,
            movieId INTEGER,
            rating REAL,
            timestamp INTEGER,
            PRIMARY KEY (userId, movieId),
            FOREIGN KEY (movieId) REFERENCES movies (movieId)
        )
    ''')

    # Tabela tags
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tags (
            userId INTEGER,
            movieId INTEGER,
            tag TEXT,
            timestamp INTEGER,
            PRIMARY KEY (userId, movieId, tag),
            FOREIGN KEY (movieId) REFERENCES movies (movieId)
        )
    ''')

    conn.commit()
    conn.close()


def load_data_from_csv():
    """Ładuje dane z plików CSV do bazy danych"""
    conn = sqlite3.connect('movies.db')
    cursor = conn.cursor()

    # Wczytaj filmy
    try:
        with open('movies.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Pomijamy nagłówek
            for row in reader:
                if len(row) >= 3:
                    cursor.execute(
                        'INSERT OR IGNORE INTO movies (movieId, title, genres) VALUES (?, ?, ?)',
                        (int(row[0]), row[1], row[2])
                    )
    except FileNotFoundError:
        print("Plik movies.csv nie został znaleziony")

    # Wczytaj linki
    try:
        with open('links.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                if len(row) >= 3:
                    cursor.execute(
                        'INSERT OR IGNORE INTO links (movieId, imdbId, tmdbId) VALUES (?, ?, ?)',
                        (int(row[0]), row[1], row[2] if row[2] != '' else None)
                    )
    except FileNotFoundError:
        print("Plik links.csv nie został znaleziony")

    # Wczytaj oceny
    try:
        with open('ratings.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                if len(row) >= 4:
                    cursor.execute(
                        'INSERT OR IGNORE INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)',
                        (int(row[0]), int(row[1]), float(row[2]), int(row[3]))
                    )
    except FileNotFoundError:
        print("Plik ratings.csv nie został znaleziony")

    # Wczytaj tagi
    try:
        with open('tags.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                if len(row) >= 4:
                    cursor.execute(
                        'INSERT OR IGNORE INTO tags (userId, movieId, tag, timestamp) VALUES (?, ?, ?, ?)',
                        (int(row[0]), int(row[1]), row[2], int(row[3]))
                    )
    except FileNotFoundError:
        print("Plik tags.csv nie został znaleziony")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_database()
    load_data_from_csv()
    print("Baza danych została utworzona i wypełniona danymi!")