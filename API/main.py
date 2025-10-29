from fastapi import FastAPI, HTTPException
from typing import List, Optional
import sqlite3
from pydantic import BaseModel

class Movie(BaseModel):
    movieId: int
    title: str
    genres: Optional[str] = None


class Link(BaseModel):
    movieId: int
    imdbId: Optional[str] = None
    tmdbId: Optional[str] = None


class Rating(BaseModel):
    userId: int
    movieId: int
    rating: float
    timestamp: int


class Tag(BaseModel):
    userId: int
    movieId: int
    tag: str
    timestamp: int

app = FastAPI()

def get_db_connection():
    """Tworzy połączenie z bazą danych"""
    conn = sqlite3.connect('movies.db')
    conn.row_factory = sqlite3.Row  # Umożliwia dostęp do kolumn przez nazwy
    return conn


def get_movies_from_db():
    """Pobiera wszystkie filmy z bazy danych"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM movies')
    movies = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return movies


def get_links_from_db():
    """Pobiera wszystkie linki z bazy danych"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM links')
    links = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return links


def get_ratings_from_db():
    """Pobiera wszystkie oceny z bazy danych"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM ratings')
    ratings = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return ratings


def get_tags_from_db():
    """Pobiera wszystkie tagi z bazy danych"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tags')
    tags = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return tags

@app.get("/")
async def root():
    return {"Welcome to Movie API with SQLite"}


@app.get("/hello")
async def hello_world():
    return {"hello": "world"}


@app.get("/movies", response_model=List[Movie])
async def get_movies():
    """Zwraca listę wszystkich filmów z bazy danych"""
    try:
        movies = get_movies_from_db()
        return movies
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd bazy danych: {str(e)}")


@app.get("/links", response_model=List[Link])
async def get_links():
    """Zwraca listę wszystkich linków z bazy danych"""
    try:
        links = get_links_from_db()
        return links
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd bazy danych: {str(e)}")


@app.get("/ratings", response_model=List[Rating])
async def get_ratings():
    """Zwraca listę wszystkich ocen z bazy danych"""
    try:
        ratings = get_ratings_from_db()
        return ratings
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd bazy danych: {str(e)}")


@app.get("/tags", response_model=List[Tag])
async def get_tags():
    """Zwraca listę wszystkich tagów z bazy danych"""
    try:
        tags = get_tags_from_db()
        return tags
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd bazy danych: {str(e)}")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)