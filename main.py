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


def fetch_single_row(query: str, params: tuple):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def execute_write(query: str, params: tuple) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    rowcount = cursor.rowcount
    conn.close()
    return rowcount


@app.get("/movies", response_model=List[Movie])
async def get_movies():
    """Zwraca listę wszystkich filmów z bazy danych"""
    try:
        movies = get_movies_from_db()
        return movies
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd bazy danych: {str(e)}")


# ============ LINKS ENDPOINTS ============

@app.get("/links", response_model=List[Link])
async def get_links():
    """Zwraca listę wszystkich linków z bazy danych"""
    try:
        links = get_links_from_db()
        return links
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd bazy danych: {str(e)}")


@app.post("/links", response_model=Link, status_code=201)
async def create_link(link: Link):
    """Tworzy nowy link w bazie danych"""
    try:
        execute_write(
            'INSERT INTO links (movieId, imdbId, tmdbId) VALUES (?, ?, ?)',
            (link.movieId, link.imdbId, link.tmdbId),
        )
        created = fetch_single_row('SELECT * FROM links WHERE movieId=?', (link.movieId,))
        return created
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Nie można utworzyć linku: {str(e)}")


@app.get("/links/{movie_id}", response_model=Link)
async def read_link(movie_id: int):
    """Zwraca link dla danego filmId"""
    link = fetch_single_row('SELECT * FROM links WHERE movieId=?', (movie_id,))
    if not link:
        raise HTTPException(status_code=404, detail="Link nie istnieje")
    return link


@app.put("/links/{movie_id}", response_model=Link)
async def update_link(movie_id: int, link: Link):
    """Aktualizuje link dla danego filmId"""
    if link.movieId != movie_id:
        raise HTTPException(status_code=400, detail="Identyfikatory nie są zgodne")
    updated = execute_write(
        'UPDATE links SET imdbId=?, tmdbId=? WHERE movieId=?',
        (link.imdbId, link.tmdbId, movie_id),
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Link nie istnieje")
    return fetch_single_row('SELECT * FROM links WHERE movieId=?', (movie_id,))


@app.delete("/links/{movie_id}")
async def delete_link(movie_id: int):
    """Usuwa link dla danego filmId"""
    deleted = execute_write('DELETE FROM links WHERE movieId=?', (movie_id,))
    if not deleted:
        raise HTTPException(status_code=404, detail="Link nie istnieje")
    return {"detail": "Link usunięty"}


# ============ RATINGS ENDPOINTS ============

@app.get("/ratings", response_model=List[Rating])
async def get_ratings():
    """Zwraca listę wszystkich ocen z bazy danych"""
    try:
        ratings = get_ratings_from_db()
        return ratings
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd bazy danych: {str(e)}")


@app.post("/ratings", response_model=Rating, status_code=201)
async def create_rating(rating: Rating):
    """Tworzy nową ocenę w bazie danych"""
    try:
        execute_write(
            'INSERT INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)',
            (rating.userId, rating.movieId, rating.rating, rating.timestamp),
        )
        created = fetch_single_row(
            'SELECT * FROM ratings WHERE userId=? AND movieId=?',
            (rating.userId, rating.movieId),
        )
        return created
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Nie można utworzyć oceny: {str(e)}")


@app.get("/ratings/{user_id}/{movie_id}", response_model=Rating)
async def read_rating(user_id: int, movie_id: int):
    """Zwraca ocenę dla danego użytkownika i filmId"""
    rating = fetch_single_row(
        'SELECT * FROM ratings WHERE userId=? AND movieId=?',
        (user_id, movie_id),
    )
    if not rating:
        raise HTTPException(status_code=404, detail="Ocena nie istnieje")
    return rating


@app.put("/ratings/{user_id}/{movie_id}", response_model=Rating)
async def update_rating(user_id: int, movie_id: int, rating: Rating):
    """Aktualizuje ocenę dla danego użytkownika i filmId"""
    if rating.userId != user_id or rating.movieId != movie_id:
        raise HTTPException(status_code=400, detail="Identyfikatory nie są zgodne")
    updated = execute_write(
        'UPDATE ratings SET rating=?, timestamp=? WHERE userId=? AND movieId=?',
        (rating.rating, rating.timestamp, user_id, movie_id),
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Ocena nie istnieje")
    return fetch_single_row(
        'SELECT * FROM ratings WHERE userId=? AND movieId=?',
        (user_id, movie_id),
    )


@app.delete("/ratings/{user_id}/{movie_id}")
async def delete_rating(user_id: int, movie_id: int):
    """Usuwa ocenę dla danego użytkownika i filmId"""
    deleted = execute_write(
        'DELETE FROM ratings WHERE userId=? AND movieId=?',
        (user_id, movie_id),
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="Ocena nie istnieje")
    return {"detail": "Ocena usunięta"}


# ============ TAGS ENDPOINTS ============

@app.get("/tags", response_model=List[Tag])
async def get_tags():
    """Zwraca listę wszystkich tagów z bazy danych"""
    try:
        tags = get_tags_from_db()
        return tags
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd bazy danych: {str(e)}")


@app.post("/tags", response_model=Tag, status_code=201)
async def create_tag(tag: Tag):
    """Tworzy nowy tag w bazie danych"""
    try:
        execute_write(
            'INSERT INTO tags (userId, movieId, tag, timestamp) VALUES (?, ?, ?, ?)',
            (tag.userId, tag.movieId, tag.tag, tag.timestamp),
        )
        created = fetch_single_row(
            'SELECT * FROM tags WHERE userId=? AND movieId=? AND tag=?',
            (tag.userId, tag.movieId, tag.tag),
        )
        return created
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Nie można utworzyć tagu: {str(e)}")


@app.get("/tags/{user_id}/{movie_id}/{tag_name}", response_model=Tag)
async def read_tag(user_id: int, movie_id: int, tag_name: str):
    """Zwraca tag dla danego użytkownika, filmId i nazwy tagu"""
    tag = fetch_single_row(
        'SELECT * FROM tags WHERE userId=? AND movieId=? AND tag=?',
        (user_id, movie_id, tag_name),
    )
    if not tag:
        raise HTTPException(status_code=404, detail="Tag nie istnieje")
    return tag


@app.put("/tags/{user_id}/{movie_id}/{tag_name}", response_model=Tag)
async def update_tag(user_id: int, movie_id: int, tag_name: str, tag: Tag):
    """Aktualizuje tag dla danego użytkownika, filmId i nazwy tagu"""
    if tag.userId != user_id or tag.movieId != movie_id or tag.tag != tag_name:
        raise HTTPException(status_code=400, detail="Identyfikatory nie są zgodne")
    updated = execute_write(
        'UPDATE tags SET timestamp=? WHERE userId=? AND movieId=? AND tag=?',
        (tag.timestamp, user_id, movie_id, tag_name),
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Tag nie istnieje")
    return fetch_single_row(
        'SELECT * FROM tags WHERE userId=? AND movieId=? AND tag=?',
        (user_id, movie_id, tag_name),
    )


@app.delete("/tags/{user_id}/{movie_id}/{tag_name}")
async def delete_tag(user_id: int, movie_id: int, tag_name: str):
    """Usuwa tag dla danego użytkownika, filmId i nazwy tagu"""
    deleted = execute_write(
        'DELETE FROM tags WHERE userId=? AND movieId=? AND tag=?',
        (user_id, movie_id, tag_name),
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="Tag nie istnieje")
    return {"detail": "Tag usunięty"}

@app.post("/movies", response_model=Movie, status_code=201)
async def create_movie(movie: Movie):
    try:
        execute_write(
            'INSERT INTO movies (movieId, title, genres) VALUES (?, ?, ?)',
            (movie.movieId, movie.title, movie.genres),
        )
        created = fetch_single_row('SELECT * FROM movies WHERE movieId=?', (movie.movieId,))
        return created
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Nie można utworzyć filmu: {str(e)}")


@app.get("/movies/{movie_id}", response_model=Movie)
async def read_movie(movie_id: int):
    movie = fetch_single_row('SELECT * FROM movies WHERE movieId=?', (movie_id,))
    if not movie:
        raise HTTPException(status_code=404, detail="Film nie istnieje")
    return movie


@app.put("/movies/{movie_id}", response_model=Movie)
async def update_movie(movie_id: int, movie: Movie):
    if movie.movieId != movie_id:
        raise HTTPException(status_code=400, detail="Identyfikatory nie są zgodne")
    updated = execute_write(
        'UPDATE movies SET title=?, genres=? WHERE movieId=?',
        (movie.title, movie.genres, movie_id),
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Film nie istnieje")
    return fetch_single_row('SELECT * FROM movies WHERE movieId=?', (movie_id,))


@app.delete("/movies/{movie_id}")
async def delete_movie(movie_id: int):
    deleted = execute_write('DELETE FROM movies WHERE movieId=?', (movie_id,))
    if not deleted:
        raise HTTPException(status_code=404, detail="Film nie istnieje")
    return {"detail": "Film usunięty"}


