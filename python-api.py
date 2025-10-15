from fastapi import FastAPI
import csv

app = FastAPI()


class Movie:
    def __init__(self, movieId: int, title: str, genres: str):
        self.movieId = movieId
        self.title = title
        self.genres = genres

    def __dict__(self):
        return {
            "movieId": self.movieId,
            "title": self.title,
            "genres": self.genres
        }


class Link:
    def __init__(self, movieId: int, imdbId: str, tmdbId: str):
        self.movieId = movieId
        self.imdbId = imdbId
        self.tmdbId = tmdbId

    def __dict__(self):
        return {
            "movieId": self.movieId,
            "imdbId": self.imdbId,
            "tmdbId": self.tmdbId
        }


class Rating:
    def __init__(self, userId: int, movieId: int, rating: float, timestamp: int):
        self.userId = userId
        self.movieId = movieId
        self.rating = rating
        self.timestamp = timestamp

    def __dict__(self):
        return {
            "userId": self.userId,
            "movieId": self.movieId,
            "rating": self.rating,
            "timestamp": self.timestamp
        }


class Tag:
    def __init__(self, userId: int, movieId: int, tag: str, timestamp: int):
        self.userId = userId
        self.movieId = movieId
        self.tag = tag
        self.timestamp = timestamp

    def __dict__(self):
        return {
            "userId": self.userId,
            "movieId": self.movieId,
            "tag": self.tag,
            "timestamp": self.timestamp
        }


# Funkcje do wczytywania danych z plików
def load_movies():
    movies = []
    try:
        with open('movies.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Pomijamy nagłówek jeśli istnieje
            for row in reader:
                if len(row) >= 3:
                    movie = Movie(
                        movieId=int(row[0]),
                        title=row[1],
                        genres=row[2]
                    )
                    movies.append(movie)
    except FileNotFoundError:
        print("Plik movies.csv nie został znaleziony")
    return movies


def load_links():
    links = []
    try:
        with open('links.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Pomijamy nagłówek jeśli istnieje
            for row in reader:
                if len(row) >= 3:
                    link = Link(
                        movieId=int(row[0]),
                        imdbId=row[1],
                        tmdbId=row[2] if row[2] != '' else None
                    )
                    links.append(link)
    except FileNotFoundError:
        print("Plik links.csv nie został znaleziony")
    return links


def load_ratings():
    ratings = []
    try:
        with open('ratings.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Pomijamy nagłówek jeśli istnieje
            for row in reader:
                if len(row) >= 4:
                    rating = Rating(
                        userId=int(row[0]),
                        movieId=int(row[1]),
                        rating=float(row[2]),
                        timestamp=int(row[3])
                    )
                    ratings.append(rating)
    except FileNotFoundError:
        print("Plik ratings.csv nie został znaleziony")
    return ratings


def load_tags():
    tags = []
    try:
        with open('tags.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Pomijamy nagłówek jeśli istnieje
            for row in reader:
                if len(row) >= 4:
                    tag = Tag(
                        userId=int(row[0]),
                        movieId=int(row[1]),
                        tag=row[2],
                        timestamp=int(row[3])
                    )
                    tags.append(tag)
    except FileNotFoundError:
        print("Plik tags.csv nie został znaleziony")
    return tags


# Wczytanie danych przy starcie aplikacji
movies_data = load_movies()
links_data = load_links()
ratings_data = load_ratings()
tags_data = load_tags()


# Endpointy
@app.get("/")
async def root():
    return {"message": "Welcome to Movie API"}


@app.get("/hello")
async def hello_world():
    return {"hello": "world"}


@app.get("/movies")
async def get_movies():
    """Zwraca listę wszystkich filmów"""
    return [movie.__dict__() for movie in movies_data]


@app.get("/links")
async def get_links():
    """Zwraca listę wszystkich linków"""
    return [link.__dict__() for link in links_data]


@app.get("/ratings")
async def get_ratings():
    """Zwraca listę wszystkich ocen"""
    return [rating.__dict__() for rating in ratings_data]


@app.get("/tags")
async def get_tags():
    """Zwraca listę wszystkich tagów"""
    return [tag.__dict__() for tag in tags_data]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
