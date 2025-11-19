from datetime import datetime, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager

# --- FastAPI ---
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

# --- Pydantic (Walidacja modeli) ---
from pydantic import BaseModel, ConfigDict

# --- JWT ---
from jose import JWTError, jwt

# --- Elementy z naszej warstwy bazy danych (users_db.py) ---
from users_db import (
    User,  # Model SQLAlchemy
    get_db,  # Zależność sesji DB
    get_password_hash,  # Pomocnik do hashowania
    verify_password,  # Pomocnik do weryfikacji hasła
    create_first_admin  # Funkcja startowa
)
from sqlalchemy.orm import Session  # Potrzebne do type hintingu


# --- Schematy Pydantic (Walidacja danych We/Wy) ---
class LoginData(BaseModel):
    username: str
    password: str


class UserCreate(BaseModel):
    username: str
    password: str
    roles: Optional[str] = 'ROLE_USER'


class UserInDB(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    username: str
    roles: str


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None
    roles: Optional[str] = None


# --- Konfiguracja Bezpieczeństwa (JWT) ---
SECRET_KEY = "super_secret_key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


# --- Funkcja pomocnicza (JWT) ---
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """ Tworzy token JWT """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "iat": datetime.utcnow()})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# --- Zależności (Dependencies) do Autoryzacji ---

# Krok 5 i 7: Weryfikacja tokena i pobranie payloadu
async def get_token_payload(token: str = Depends(oauth2_scheme)) -> dict:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Nie można zweryfikować poświadczeń",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        return payload
    except JWTError:
        raise credentials_exception


# Krok 6: Weryfikacja roli ADMINA
async def get_current_admin_user(payload: dict = Depends(get_token_payload)):
    roles = payload.get("roles")
    if roles != "ROLE_ADMIN":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Wymagana rola administratora!",
        )
    return payload


# Zależność do pobierania *obiektu* User z bazy na podstawie tokena
async def get_current_user(payload: dict = Depends(get_token_payload), db: Session = Depends(get_db)) -> User:
    username = payload.get("sub")
    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nie można znaleźć użytkownika z tokena",
        )
    return user


# --- Logika startowa (Lifespan) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- Uruchamianie aplikacji: Sprawdzanie administratora... ---")
    create_first_admin()  # Wywołujemy funkcję z users_db.py
    yield
    print("--- Zamykanie aplikacji ---")


# --- Główna Aplikacja FastAPI ---
app = FastAPI(lifespan=lifespan)


# --- Endpointy API ---

# Krok 1 & 3: Endpoint /login do uwierzytelniania
@app.post("/login", response_model=Token)
async def login(data: LoginData, db: Session = Depends(get_db)):
    # Krok 3: Weryfikacja usera w bazie
    user = db.query(User).filter(User.username == data.username).first()

    if not user or not verify_password(data.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Błędna nazwa użytkownika lub hasło",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Krok 6 (część): Dodanie ról do payloadu
    token_data = {
        "sub": user.username,
        "roles": user.roles
    }
    access_token = create_access_token(data=token_data)
    return {"access_token": access_token, "token_type": "bearer"}


# Krok 4, 5, 6: Endpoint POST /users (tworzenie użytkownika)
@app.post("/users", response_model=UserInDB)
async def create_user(
        user_data: UserCreate,
        db: Session = Depends(get_db),
        admin_payload: dict = Depends(get_current_admin_user)  # Krok 5 i 6
):
    db_user = db.query(User).filter(User.username == user_data.username).first()
    if db_user:
        raise HTTPException(status_code=409, detail="Użytkownik o tej nazwie już istnieje")

    hashed_password = get_password_hash(user_data.password)
    new_user = User(
        username=user_data.username,
        password=hashed_password,
        roles=user_data.roles
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user


# Krok 5 i 7: Endpoint /user_details (zwraca dane z payloadu)
@app.get("/user_details")
async def get_user_details(
        payload: dict = Depends(get_token_payload)  # Krok 5
):
    return {"payload": payload}  # Krok 7


# Krok 5: Przykładowy dodatkowy zabezpieczony endpoint
@app.get("/protected")
async def read_protected_route(
        current_user: User = Depends(get_current_user)  # Krok 5
):
    return {"message": f"Witaj {current_user.username}! Masz dostęp do chronionego zasobu."}