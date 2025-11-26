import pytest
import sqlite3
import time
from fastapi.testclient import TestClient
from main import app, get_db_connection


@pytest.fixture(scope="function")
def client():
    return TestClient(app)


def _wait_and_clear_db():
    time.sleep(0.2)
    retries = 15
    while retries > 0:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM tags')
            cursor.execute('DELETE FROM ratings')
            cursor.execute('DELETE FROM links')
            cursor.execute('DELETE FROM movies')
            conn.commit()
            conn.close()
            return
        except sqlite3.OperationalError:
            if conn:
                try:
                    conn.close()
                except:
                    pass
            retries -= 1
            time.sleep(0.25)
    raise sqlite3.OperationalError("Could not acquire database lock")


def _populate_test_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    test_movies = [
        (1, 'Test Movie 1', 'Action|Adventure'),
        (2, 'Test Movie 2', 'Drama|Comedy'),
        (3, 'Test Movie 3', 'Thriller'),
    ]
    for movie in test_movies:
        cursor.execute(
            'INSERT INTO movies (movieId, title, genres) VALUES (?, ?, ?)',
            movie
        )
    conn.commit()

    test_links = [
        (1, 'tt0111161', '278'),
        (2, 'tt0068646', '238'),
    ]
    for link in test_links:
        cursor.execute(
            'INSERT INTO links (movieId, imdbId, tmdbId) VALUES (?, ?, ?)',
            link
        )
    conn.commit()

    # Oceny
    test_ratings = [
        (1, 1, 5.0, 1000000),
        (2, 1, 4.5, 1000001),
        (1, 2, 3.5, 1000002),
    ]
    for rating in test_ratings:
        cursor.execute(
            'INSERT INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)',
            rating
        )
    conn.commit()

    test_tags = [
        (1, 1, 'epic', 2000000),
        (2, 1, 'masterpiece', 2000001),
    ]
    for tag in test_tags:
        cursor.execute(
            'INSERT INTO tags (userId, movieId, tag, timestamp) VALUES (?, ?, ?, ?)',
            tag
        )
    conn.commit()
    conn.close()


@pytest.fixture(scope="function")
def setup_test_db():
    _wait_and_clear_db()
    _populate_test_data()

    yield

    # Teardown
    try:
        _wait_and_clear_db()
    except Exception:
        pass


# ============ MOVIES TESTS ============

class TestMovies:
    """Testy dla endpointów /movies"""

    def test_get_all_movies(self, client, setup_test_db):
        """Test GET /movies - zwraca listę wszystkich filmów"""
        response = client.get("/movies")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        assert all('movieId' in movie for movie in data)

    def test_get_all_movies_content(self, client, setup_test_db):
        """Test GET /movies - zawartość"""
        response = client.get("/movies")
        data = response.json()
        assert data[0]['movieId'] == 1
        assert data[0]['title'] == 'Test Movie 1'
        assert data[1]['movieId'] == 2

    def test_create_movie(self, client, setup_test_db):
        """Test POST /movies - tworzy film"""
        new = {"movieId": 100, "title": "New", "genres": "Action"}
        resp = client.post("/movies", json=new)
        assert resp.status_code == 201
        assert resp.json()['movieId'] == 100

        # Weryfikacja
        all_movies = client.get("/movies").json()
        assert len(all_movies) == 4

    def test_create_movie_duplicate_fails(self, client, setup_test_db):
        """Test POST /movies - duplicate fails"""
        new = {"movieId": 1, "title": "Dup", "genres": "Drama"}
        resp = client.post("/movies", json=new)
        assert resp.status_code == 400

    def test_get_single_movie(self, client, setup_test_db):
        """Test GET /movies/{movie_id}"""
        resp = client.get("/movies/1")
        assert resp.status_code == 200
        assert resp.json()['movieId'] == 1

    def test_get_single_movie_not_found(self, client, setup_test_db):
        """Test GET /movies/{movie_id} - 404"""
        resp = client.get("/movies/999")
        assert resp.status_code == 404

    def test_update_movie(self, client, setup_test_db):
        """Test PUT /movies/{movie_id}"""
        upd = {"movieId": 1, "title": "Updated", "genres": "Drama"}
        resp = client.put("/movies/1", json=upd)
        assert resp.status_code == 200
        assert resp.json()['title'] == "Updated"

        verify = client.get("/movies/1").json()
        assert verify['title'] == "Updated"

    def test_update_movie_not_found(self, client, setup_test_db):
        """Test PUT /movies/{movie_id} - 404"""
        upd = {"movieId": 999, "title": "Test", "genres": "Drama"}
        resp = client.put("/movies/999", json=upd)
        assert resp.status_code == 404

    def test_update_movie_id_mismatch(self, client, setup_test_db):
        """Test PUT /movies - ID mismatch"""
        upd = {"movieId": 2, "title": "Test", "genres": "Drama"}
        resp = client.put("/movies/1", json=upd)
        assert resp.status_code == 400

    def test_delete_movie(self, client, setup_test_db):
        """Test DELETE /movies/{movie_id}"""
        resp = client.delete("/movies/1")
        assert resp.status_code == 200

        verify = client.get("/movies/1")
        assert verify.status_code == 404

    def test_delete_movie_not_found(self, client, setup_test_db):
        """Test DELETE /movies - 404"""
        resp = client.delete("/movies/999")
        assert resp.status_code == 404


# ============ LINKS TESTS ============

class TestLinks:
    """Testy dla endpointów /links"""

    def test_get_all_links(self, client, setup_test_db):
        """Test GET /links"""
        resp = client.get("/links")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_get_all_links_content(self, client, setup_test_db):
        """Test GET /links - content"""
        resp = client.get("/links")
        data = resp.json()
        assert data[0]['movieId'] == 1
        assert data[0]['imdbId'] == 'tt0111161'

    def test_create_link(self, client, setup_test_db):
        """Test POST /links"""
        new = {"movieId": 3, "imdbId": "tt0468569", "tmdbId": "155"}
        resp = client.post("/links", json=new)
        assert resp.status_code == 201

        all_links = client.get("/links").json()
        assert len(all_links) == 3

    def test_get_single_link(self, client, setup_test_db):
        """Test GET /links/{movie_id}"""
        resp = client.get("/links/1")
        assert resp.status_code == 200
        assert resp.json()['movieId'] == 1

    def test_get_single_link_not_found(self, client, setup_test_db):
        """Test GET /links - 404"""
        resp = client.get("/links/999")
        assert resp.status_code == 404

    def test_update_link(self, client, setup_test_db):
        """Test PUT /links/{movie_id}"""
        upd = {"movieId": 1, "imdbId": "tt9999", "tmdbId": "9999"}
        resp = client.put("/links/1", json=upd)
        assert resp.status_code == 200
        assert resp.json()['imdbId'] == "tt9999"

    def test_delete_link(self, client, setup_test_db):
        """Test DELETE /links/{movie_id}"""
        resp = client.delete("/links/1")
        assert resp.status_code == 200

        verify = client.get("/links/1")
        assert verify.status_code == 404


# ============ RATINGS TESTS ============

class TestRatings:
    """Testy dla endpointów /ratings"""

    def test_get_all_ratings(self, client, setup_test_db):
        """Test GET /ratings"""
        resp = client.get("/ratings")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3

    def test_get_all_ratings_content(self, client, setup_test_db):
        """Test GET /ratings - content"""
        resp = client.get("/ratings")
        data = resp.json()
        assert data[0]['userId'] == 1
        assert data[0]['rating'] == 5.0

    def test_create_rating(self, client, setup_test_db):
        """Test POST /ratings"""
        new = {"userId": 3, "movieId": 3, "rating": 4.0, "timestamp": 1000100}
        resp = client.post("/ratings", json=new)
        assert resp.status_code == 201

        all_ratings = client.get("/ratings").json()
        assert len(all_ratings) == 4

    def test_get_single_rating(self, client, setup_test_db):
        """Test GET /ratings/{user_id}/{movie_id}"""
        resp = client.get("/ratings/1/1")
        assert resp.status_code == 200
        assert resp.json()['rating'] == 5.0

    def test_get_single_rating_not_found(self, client, setup_test_db):
        """Test GET /ratings - 404"""
        resp = client.get("/ratings/999/999")
        assert resp.status_code == 404

    def test_update_rating(self, client, setup_test_db):
        """Test PUT /ratings/{user_id}/{movie_id}"""
        upd = {"userId": 1, "movieId": 1, "rating": 3.0, "timestamp": 1000010}
        resp = client.put("/ratings/1/1", json=upd)
        assert resp.status_code == 200
        assert resp.json()['rating'] == 3.0

    def test_delete_rating(self, client, setup_test_db):
        """Test DELETE /ratings/{user_id}/{movie_id}"""
        resp = client.delete("/ratings/1/1")
        assert resp.status_code == 200

        verify = client.get("/ratings/1/1")
        assert verify.status_code == 404


# ============ TAGS TESTS ============

class TestTags:
    """Testy dla endpointów /tags"""

    def test_get_all_tags(self, client, setup_test_db):
        """Test GET /tags"""
        resp = client.get("/tags")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_get_all_tags_content(self, client, setup_test_db):
        """Test GET /tags - content"""
        resp = client.get("/tags")
        data = resp.json()
        assert data[0]['userId'] == 1
        assert data[0]['tag'] == 'epic'

    def test_create_tag(self, client, setup_test_db):
        """Test POST /tags"""
        new = {"userId": 3, "movieId": 2, "tag": "classic", "timestamp": 2000100}
        resp = client.post("/tags", json=new)
        assert resp.status_code == 201

        all_tags = client.get("/tags").json()
        assert len(all_tags) == 3

    def test_get_single_tag(self, client, setup_test_db):
        """Test GET /tags/{user_id}/{movie_id}/{tag_name}"""
        resp = client.get("/tags/1/1/epic")
        assert resp.status_code == 200
        assert resp.json()['tag'] == 'epic'

    def test_get_single_tag_not_found(self, client, setup_test_db):
        """Test GET /tags - 404"""
        resp = client.get("/tags/999/999/nonexistent")
        assert resp.status_code == 404

    def test_update_tag(self, client, setup_test_db):
        """Test PUT /tags/{user_id}/{movie_id}/{tag_name}"""
        upd = {"userId": 1, "movieId": 1, "tag": "epic", "timestamp": 2000010}
        resp = client.put("/tags/1/1/epic", json=upd)
        assert resp.status_code == 200
        assert resp.json()['timestamp'] == 2000010

    def test_delete_tag(self, client, setup_test_db):
        """Test DELETE /tags/{user_id}/{movie_id}/{tag_name}"""
        resp = client.delete("/tags/1/1/epic")
        assert resp.status_code == 200

        verify = client.get("/tags/1/1/epic")
        assert verify.status_code == 404


# ============ INTEGRATION TESTS ============

class TestIntegration:
    """Testy integracyjne"""

    def test_full_movie_workflow(self, client, setup_test_db):
        """Test pełnego CRUD workflow dla filmów"""
        # Create
        new = {"movieId": 50, "title": "Workflow", "genres": "Action"}
        create = client.post("/movies", json=new)
        assert create.status_code == 201

        # Read
        read = client.get("/movies/50")
        assert read.status_code == 200

        # Update
        update_data = {"movieId": 50, "title": "Updated", "genres": "Drama"}
        update = client.put("/movies/50", json=update_data)
        assert update.status_code == 200

        # Delete
        delete = client.delete("/movies/50")
        assert delete.status_code == 200

        # Verify deletion
        final = client.get("/movies/50")
        assert final.status_code == 404

    def test_list_consistency_after_modifications(self, client, setup_test_db):
        """Test konsystencji listy po modyfikacjach"""
        initial = len(client.get("/movies").json())

        client.post("/movies", json={"movieId": 60, "title": "Test", "genres": "Action"})
        after_add = len(client.get("/movies").json())
        assert after_add == initial + 1

        client.delete("/movies/60")
        after_delete = len(client.get("/movies").json())
        assert after_delete == initial

    def test_rating_workflow(self, client, setup_test_db):
        """Test workflow dla ratingu"""
        # Create
        new = {"userId": 5, "movieId": 1, "rating": 3.5, "timestamp": 1000100}
        create = client.post("/ratings", json=new)
        assert create.status_code == 201

        # Read and verify
        read = client.get("/ratings/5/1")
        assert read.status_code == 200
        assert read.json()['rating'] == 3.5

        # Update
        upd = {"userId": 5, "movieId": 1, "rating": 4.5, "timestamp": 1000101}
        update = client.put("/ratings/5/1", json=upd)
        assert update.status_code == 200

        # Delete
        delete = client.delete("/ratings/5/1")
        assert delete.status_code == 200

        # Verify deletion
        verify = client.get("/ratings/5/1")
        assert verify.status_code == 404

    def test_multiple_resources_independent_crud(self, client, setup_test_db):
        """Test niezależności CRUD operacji między zasobami"""
        # Dodaj film
        movie = {"movieId": 70, "title": "Multi", "genres": "Action"}
        client.post("/movies", json=movie)

        # Dodaj link
        link = {"movieId": 70, "imdbId": "tt0123456", "tmdbId": "123"}
        client.post("/links", json=link)

        # Dodaj rating
        rating = {"userId": 10, "movieId": 70, "rating": 5.0, "timestamp": 1000200}
        client.post("/ratings", json=rating)

        # Sprawdź czy wszystkie zasoby są niezależne
        assert client.get("/movies/70").status_code == 200
        assert client.get("/links/70").status_code == 200
        assert client.get("/ratings/10/70").status_code == 200

        # Usuń film
        client.delete("/movies/70")

        movie_deleted = client.get("/movies/70").status_code == 404
        assert movie_deleted

