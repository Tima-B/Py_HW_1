from typing import List

import databases
import sqlalchemy
import datetime
from fastapi import FastAPI
from pydantic import BaseModel

DB_USER = "postgres"
DB_NAME = "study"
DB_PASSWORD = "postgres"
DB_HOST = "127.0.0.1"

# SQLAlchemy specific code, as with any other app
DATABASE_URL = "sqlite:///./test.db"
# DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
print(DATABASE_URL)
database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

# Раскомментировать следующее в случае postgres
# engine = create_engine(
#     SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
# )
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base = declarative_base()
# также можно посмотреть этот репозиторий https://github.com/tiangolo/full-stack-fastapi-postgresql
notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)
# Создаем таблицу Store (Магазины)
store = sqlalchemy.Table(
    "store",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("address", sqlalchemy.String),
)
# Создаем таблицу item (товарные позиции (уникальные наименования))
item = sqlalchemy.Table(
    "item",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String, unique=True),
    sqlalchemy.Column("price", sqlalchemy.Float),
)
# Создаем таблицу sales (Продажи)
sales = sqlalchemy.Table(
    "sales",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("sale_time", sqlalchemy.DateTime, nullable=False, default=datetime.datetime.now()),
    sqlalchemy.Column("item_id", sqlalchemy.ForeignKey('item.id')),
    sqlalchemy.Column("store_id", sqlalchemy.ForeignKey('store.id')),
)


engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
    # Уберите параметр check_same_thread когда база не sqlite
)
metadata.create_all(engine)

"""
class NoteIn(BaseModel):
    text: str
    completed: bool


class Note(BaseModel):
    id: int
    text: str
    completed: bool
"""


class StoreIn(BaseModel):
    address: str


class Store(BaseModel):
    id: int
    address: str


class ItemIn(BaseModel):
    name: str
    price: float


class Item(BaseModel):
    id: int
    name: str
    price: float


class SaleIn(BaseModel):
    sale_time: datetime.datetime
    item_id: int
    store_id: int


class Sale(BaseModel):
    id: int
    sale_time: datetime.datetime
    item_id: int
    store_id: int


class StoreTop(BaseModel):
    id_store: int
    address_store: str
    sum_item_price: float


class ItemTop(BaseModel):
    id: int
    name: str
    item_count: int


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

"""
@app.get("/notes/", response_model=List[Note])
async def read_notes():
    query = notes.select()
    return await database.fetch_all(query)
"""


# Запрос списка магазинов
@app.get("/store/")
async def read_store():
    query = store.select()
    return await database.fetch_all(query)


# Запрос списка товаров
@app.get('/item/')
async def read_item():
    query = item.select()
    return await database.fetch_all(query)


# Запрос TOP-10 магазиов по выручке в месяц
@app.get("/stores/top/", response_model=List[StoreTop])
async def top_stores():
    query = f"""
        select 
            store.id as id_store,
            store.address as address_store, 
            sum(item.price) as sum_item_price,
        from sale
            join
                store
                on store.id=sale.store_id 
            join
                item	
                on item.id=sale.item_id
        where sale.sale_time>=now(), interval 1 month
        group by id_store, address_store
        order by sum_item_price desc limit 10
        """
    return await database.fetch_all(query)


# Запрос TOP-10 самых продаваемых товаров
@app.get("/items/top/", response_model=List[ItemTop])
async def top_items():
    query = f"""
        SELECT
            COUNT(1) AS item_count,
            (SELECT id, name FROM item
                WHERE item.id=sale.item_id)
            FROM sale
        GROUP BY item.id, item.name
        ORDER BY item_count DESC LIMIT 10
        """
    return await database.fetch_all(query)


"""
@app.post("/notes/", response_model=Note)
async def create_note(note: NoteIn):
    query = notes.insert().values(text=note.text, completed=note.completed)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}
"""


# Добавление в таблицу магазинов
@app.post("/store/", response_model=Store)
async def create_store(note: StoreIn):
    query = store.insert().values(address=note.address)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}


# Добавление в таблицу товаров
@app.post("/item/", response_model=Item)
async def create_item(note: ItemIn):
    query = item.insert().values(name=note.name, price=note.price)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}


# Добавление в таблицу продаж
@app.post("/sale/", response_model=Sale)
async def create_sale(note: SaleIn):
    query = store.insert().values(sale_time=note.sale_time, item_id=note.item_id, store_id=note.store_id)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}


# запуск: uvicorn <filename>:application
