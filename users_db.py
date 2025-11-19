import bcrypt
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session

# --- Konfiguracja Bazy Danych ---
DATABASE_URL = "sqlite:///./auth_app.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# --- Bazowy model SQLAlchemy ---
class Base(DeclarativeBase):
    pass


# --- Krok 2 i 6: Model Użytkownika (tabela 'user') ---
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    password = Column(String, nullable=False)  # Zahasnowane
    roles = Column(String, nullable=False, default='ROLE_USER')


# --- Utworzenie tabel w bazie ---
Base.metadata.create_all(bind=engine)


# --- Funkcje Pomocnicze (związane z bazą i modelem User) ---

def get_db():
    """ Zależność (Dependency) dostarczająca sesję Bazy Danych """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_password_hash(password: str) -> str:
    """ Haszuje hasło """
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """ Weryfikuje hasło z hashem """
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))


def create_first_admin():
    """
    Tworzy użytkownika 'admin' przy starcie aplikacji,
    jeśli żaden użytkownik admin jeszcze nie istnieje.
    """
    db = SessionLocal()
    admin_user = db.query(User).filter(User.roles == "ROLE_ADMIN").first()

    if not admin_user:
        admin_pass = "admin123"
        hashed_password = get_password_hash(admin_pass)

        admin = User(
            username="admin",
            password=hashed_password,
            roles="ROLE_ADMIN"
        )
        db.add(admin)
        db.commit()
        print("--- Utworzono użytkownika admin ---")
        print(f"--- Login: admin")
        print(f"--- Hasło: {admin_pass}")
        print("---------------------------------")
    else:
        print("--- Użytkownik admin już istnieje. Pomijanie tworzenia. ---")

    db.close()