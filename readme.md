Запуск программы из терминала PyCharm:
py -m uvicorn HW_1:app
Адрес: http://127.0.0.1:8000


Post:
Создение нового магазина
/store/
address: str

Создение нового товара
/item/
name: str
price: float

Создение новой продажи
/sales/
item_id: int
store_id: int


Get:
Список магазиов
/store/
id: int
address: str

Список товаров
/item/
id: int
name: str
price: float

Список продаж
/sales/ (вне задания - создано для контроля работы)
id: int
sale_time: datetime
item_id: int
store_id: int

Топ-10 самых прибыльных магазинов за месяц
/store/top/
id_store: int
address_store: str
sum_item_price: float

Топ-10 самых продаваемых товаров
/item/Top/
ItemID: int
Name: str
item_count: int