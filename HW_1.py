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


class SalesIn(BaseModel):
    sale_time = datetime.datetime.now()
    item_id: int
    store_id: int


class Sales(BaseModel):
    id: int
    sale_time: datetime.datetime
    item_id: int
    store_id: int


class StoreTop(BaseModel):
    id_store: int
    address_store: str
    sum_item_price: float


class ItemTop(BaseModel):
    ItemID: int
    Name: str
    item_count: int


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


# Запрос списка магазинов
@app.get("/store/")
async def read_store():
    query = store.select()
    return await database.fetch_all(query)


# Запрос списка товаров
@app.get("/item/")
async def read_item():
    query = item.select()
    return await database.fetch_all(query)


# Запрос списка продаж
@app.get("/sales/")
async def read_sales():
    query = sales.select()
    return await database.fetch_all(query)


# Запрос TOP-10 магазиов по выручке в месяц
@app.get("/stores/top/", response_model=List[StoreTop])
async def top_stores():
    query = f"""
        select 
            store.id as id_store,
            store.address as address_store, 
            sum(item.price) as sum_item_price
        from sales
            join
                store
                on store.id=sales.store_id
            join
                item	
                on item.id=sales.item_id     
        where sales.sale_time>=date('now','-1 month')
        group by id_store, address_store
        order by sum_item_price desc limit 10
        """
    return await database.fetch_all(query)


# Запрос TOP-10 самых продаваемых товаров
@app.get("/items/top/", response_model=List[ItemTop])
async def top_items():
    query = f"""
       select
            count(1) as item_count,
            item.id as ItemID,
            item.name as Name
        from sales
            join
                item
                on item.id=sales.item_id
        group by itemID, Name
        order by item_count DESC limit 10
        """
    return await database.fetch_all(query)


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
@app.post("/sales/", response_model=Sales)
async def create_sale(note: SalesIn):
    query = sales.insert().values(sale_time=note.sale_time, item_id=note.item_id, store_id=note.store_id)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}

# запуск: uvicorn <filename>:application
