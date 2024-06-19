import asyncio
import aiosqlite
import os, re, hashlib, json
from datetime import datetime, timedelta, timezone
from sortedcontainers import SortedDict

from .LockManager import lock_manager

class MDBAsync:
    time_format = "%Y-%m-%d %H:%M:%S"
    indices = ["id"]
    db_tables = {
        # ТАБЛИЦА [Аккаунты]
        "accounts": {
            "columns": {
                # По умолчанию
                "basic": {
                    "type": "INTEGER",
                    "default": 0
                },
                # Тип
                "type": {
                    "type": "TEXT",
                    "required": True
                },
                # Наименование
                "name": {
                    "type": "TEXT",
                    "required": True
                },
                # Данные для авторизации
                "auth": {
                    "type": "TEXT",
                    "required": True 
                },
                "running":{
                    "type": "INTEGER",
                    "default": 0
                },
                # Ошибка
                "error": {
                    "type": "TEXT",
                    "default": 0
                },
                # Временная метка
                "time_stamp": {
                    "type": "TEXT",
                    "default": "CURRENT_TIMESTAMP"
                }
            },
            "constraint": {
                "auth": ["auth"]
            }
        }
    }
    
    @staticmethod
    def md5(string:str) -> str:
        return hashlib.md5(bytes(string, 'utf-8')).hexdigest()

    @staticmethod
    def flt(num:float) -> float:
        return float(str(num)[0:7])

    db_column_names = {}
    def __init__(self, path:str='Main.db') -> None:
        self.path = path
        self.db_column_names = {}
        asyncio.run(self.connect())

    def time_to_datetime(self, time):
        dt = datetime.fromtimestamp(time / 1000.0, tz=timezone.utc)
        return dt.strftime(self.time_format)
    
    def subtract_datetime(self, date:str, days:int):
        new_date = datetime.strptime(date, self.time_format) - timedelta(days=days)
        return new_date.strftime(self.time_format)
    
    # Вычисляем разницу в днях
    def diff_datetime(self, date_a, date_b) -> int:
        date1 = datetime.strptime(date_a, self.time_format)
        date2 = datetime.strptime(date_b, self.time_format)
        return (date2 - date1).days
    
    def s(self, obj:dict) -> dict:
        #return {k: v for k, v in sorted(obj.items(), key=lambda item: item[1])}
        return {k: v for k, v in sorted(obj.items(), key=lambda item: str(item[1]))}
    
    def intersect_key(self, d1:dict, d2:dict) -> dict:
        return {key: d1[key] for key in d1.keys() & d2.keys()}
    
    async def connect(self) -> None:
        try:
            async with aiosqlite.connect(self.path) as db:
                await db.close()
                
            for table in self.db_tables.keys():
                rows_list  = self.indices + list(self.db_tables[table]["columns"].keys()) # ["id", ...]
                self.db_column_names[table] = {value: index for index, value in enumerate(rows_list)}

            if os.stat(self.path).st_size == 0:
                await self.create_tables(self.db_tables)
        except Exception as e:
            print(f"Error (connect): {e}")
            print("-" * 50)

    async def execute(self, sql:str, params:tuple|list|None=None, close=True) -> dict|None:
        conn = await aiosqlite.connect(self.path)
        csr = await conn.cursor()
        try:
            sett = await ((csr.executemany(sql, params) if isinstance(params, list) else csr.execute(sql, params)) if params else csr.execute(sql))
            await conn.commit()
        except Exception as e:
            print(sql)
            print(f"Error (execute): {e}")
            print("-" * 50)
        finally:
            if close:
                await conn.close()
            else:
                return {
                    "conn": conn,
                    "set": sett
                }

    async def create_tables(self, db_tables:dict) -> None:
        for table in db_tables.keys():
            columns = ""
            for ind in self.indices:
                if columns:
                    columns += ", "
                columns += f"`{ind}` INTEGER PRIMARY KEY"

            for column in db_tables[table]["columns"].keys():
                _end = ""
                if "required" in db_tables[table]['columns'][column]:
                    _end = " NOT NULL"
                elif "default" in db_tables[table]['columns'][column]:
                    _end = f" DEFAULT {db_tables[table]['columns'][column]['default']}"

                columns += f",`{column}` {db_tables[table]['columns'][column]['type']}{_end}"

            if "constraint" in db_tables[table]:
                for constraint in db_tables[table]["constraint"].keys():
                    _unique = "`,`".join(db_tables[table]["constraint"][constraint])
                    columns +=  f",CONSTRAINT `{constraint}` UNIQUE (`{_unique}`)"
            
            await self.execute(sql=f"CREATE TABLE IF NOT EXISTS `{table}` ({columns});")

    # COLUMNS
    def c(self, table_name:str, columns:str|dict|list|None=None) -> dict:
        if not columns:
            return {
                "select": "*",
                "indices": self.db_column_names[table_name]
            }
        
        columnss = {}
        columns2 = ''

        if isinstance(columns, dict):
            columns2 = ','.join(f"{k} {v}" for k, v in columns.items())
        elif isinstance(columns, list):
            columns2 = ','.join(f"`{col}`" for col in columns)
        else:
            columns2 = columns

        clean_columns = re.sub(r'[`"\']', '', columns2)
        columns_list = re.split(r'\s*,+\s*', clean_columns)
        columns_list = list(filter(None, columns_list))

        for col in columns_list:
            spcol = col.split()
            formatted_col = ' '.join(f"`{v}`" if not re.search(r'[\.\(\)]+', v) else v for v in spcol)
            columnss[spcol[-1]] = formatted_col

        return {
            "select": ','.join(columnss.values()),
            "indices": {value: index for index, value in enumerate(columnss.keys())}
        }

    # SET
    def s(self, sets:str|dict|None=None) -> dict|None:
        if not sets: return

        conditions = []
        params = []
        for k, v in sets.items():
            conditions.append(f"`{k}` = ?")
            params.append(v)

        return {
            "sql": ', '.join(conditions),
            "params": tuple(params)
        }
    
    # WHERE
    def w(self, wheres:str|dict|None=None) -> dict:
        if not wheres:
            return {"sql": "", "params": ()}

        conditions = []
        params = []
        for k, v in wheres.items():
            if isinstance(v, list):
                placeholders = ",".join(["?"] * len(v))
                conditions.append(f"`{k}` IN ({placeholders})")
                params.extend(v)
            else:
                conditions.append(f"`{k}` = ?")
                params.append(v)
        return {
            "sql": f" WHERE {' AND '.join(conditions)}",
            "params": tuple(params)
        }
    
    # ORDER BY
    def o(self, orders:dict={}) -> str:
        if not orders:
            return ''
        
        order_clauses = [f"`{k}` {v}" for k, v in orders.items()]
        return f" ORDER BY {', '.join(order_clauses)}"
    
    # LIMIT
    def l(self, limit:list|int=[]) -> str:
        if isinstance(limit, int):
            limit = [limit]

        return f" LIMIT {', '.join(map(str, limit[:2]))}" if limit else ''
    
    # cur.execute("SELECT * FROM `test` WHERE `p` = ?", (1,))
    def select(self, table_name:str, columns:str|dict|list|None=None, wheres:str|dict|None=None, orders:dict={}, limit:list|int=[]) -> dict:
        columns = self.c(table_name, columns)
        wheres = self.w(wheres)
        orders = self.o(orders)
        limit = self.l(limit)

        return {
            "sql": f"SELECT {columns['select']} FROM `{table_name}`{wheres['sql']}{orders}{limit}",
            "params": wheres['params']
        }
    
    async def sqlobj(self, table_name:str, columns:str|dict|list|None=None, wheres:str|dict|None=None, orders:dict={}, limit:list|int=[], close:bool=True) -> dict|None:
        if table_name in self.db_column_names:
            select = self.select(table_name, columns, wheres, orders, limit)
            execute = await self.execute(sql=select["sql"], params=select["params"], close=close)
            return None if execute is None else {**execute, **self.c(table_name, columns)}
    
    async def addrows(self, table_name:str, values:list=[], ids=False) -> None:
        if table_name not in self.db_column_names or not values:
            return
        
        for vals in values:
            for ind in self.indices:
                if ind in vals:
                    del vals[ind]

        keys = [key for key in values[0] if key in self.db_column_names[table_name]]
        placeholders = ", ".join(["?"] * len(keys))
        params = [tuple(d[key] for key in keys) for d in values]
        keys_str = "`, `".join(keys)
        query = f"INSERT OR IGNORE INTO `{table_name}` (`{keys_str}`) VALUES ({placeholders})"

        if ids is not False:
            obj = await self.execute(sql=query, params=params, close=False)
            ids.append((await obj["set"]).lastrowid)
            await obj["conn"].close()
            return ids
        
        await self.execute(sql=query, params=params)

    async def delrows(self, table_name:str, wheres=None, limit:list|int=[]):
        wheres = self.w(wheres)
        limit = self.l(limit)
        await self.execute(f"DELETE FROM `{table_name}`{wheres['sql']}{limit};", wheres['params'])

    async def setone(self, table_name:str, sets:dict, wheres:dict=None):
        if table_name in self.db_column_names:
            if wheres is None:
                wheres = {}
            assoc = {}

            for k in sets:
                if k in self.db_column_names[table_name]:
                    assoc[k] = sets[k]
                    # Переносим в условия записи индексы полей
                    if k in self.indices:
                        wheres[k] = assoc[k]

            # Запрещаем менять индексы
            for ind in self.indices:
                if ind in assoc:
                    del assoc[ind]

            sets = self.s(assoc)
            wheres = self.w(wheres)
            if sets:
                await self.execute(f"UPDATE `{table_name}` SET {sets['sql']}{wheres['sql']}", sets['params'] + wheres['params'])
            return True

    async def getone(self, table_name:str, columns:str|dict|list|None=None, wheres:str|dict|None=None, orders:dict={}) -> dict|None:
        if table_name in self.db_column_names:
            assoc = {}
            obj = await self.sqlobj(table_name, columns, wheres, orders, 1, False)
            if obj:
                if "set" in obj and obj["set"]:
                    row = await obj["set"].fetchone()
                    if row:
                        for key in obj["indices"].keys():
                            assoc[key] = row[obj["indices"][key]]
                await obj["conn"].close()
            return assoc

class MDBAsyncObj(MDBAsync):
    def __init__(self, path:str='Main.db') -> None:
        super().__init__(path)
        asyncio.run(self.update_obj())
        self.updating_obj = False

    @staticmethod # Сравниваем второй обьект с первым
    def compare_objs(obj0:dict, obj1:dict) -> bool:
        for k in obj1:
            if not k in obj0: continue
            if not obj1[k] == obj0[k]: return True
        return False
    
    @staticmethod # Преобразовываем в json обьект
    def loads_obj(variable):
        if isinstance(variable, str):
            variable = variable.strip()
            if re.match(r'^\{.*\}$', variable) or re.match(r'^\[.*\]$', variable):
                try:
                    json_obj = json.loads(variable)
                    return json_obj
                except ValueError:
                    pass
        return variable
        
    @staticmethod # Преобразовываем json обьект в строку
    def dumps_obj(obj):
        return json.dumps(obj) if isinstance(obj, (list|dict)) else obj

    async def update_obj(self):
        self.obj = {}
        for table in self.db_tables:
            self.obj[table] = SortedDict()
            obj = await self.sqlobj(table, close=False)
            if obj:
                if "set" in obj and obj["set"]:
                    for row in await obj["set"].fetchall():
                        assoc = {}
                        for key in obj["indices"].keys():
                            col = row[obj["indices"][key]]
                            assoc[key] = self.loads_obj(col)

                        if hasattr(self, "indices_obj") and table in self.indices_obj:
                            ind = self.indices_obj[table]
                            col = ind["columns"][0]
                            if "type" in ind and ind["type"] == list:
                                if not assoc[col] in self.obj[table]:
                                    self.obj[table][assoc[col]] = []
                                self.obj[table][assoc[col]].append(assoc)
                            else:
                                self.obj[table][assoc[col]] = assoc
                        else:
                            # next с итератором, не требующий создания списка, будет быстрее
                            key = next(iter(self.db_tables[table]["constraint"].keys()))
                            self.obj[table][assoc[key]] = assoc
                await obj["conn"].close()

    async def set_obj(self, t:str, obj:dict):
        async with lock_manager.this():
            if t not in self.db_tables:
                return
            
            try:
                ind = self.indices_obj[t] if hasattr(self, "indices_obj") and t in self.indices_obj else next(iter(self.db_tables[t]["constraint"].keys()))
                col = ind["columns"][0] if isinstance(ind, dict) else ind

                if "type" in ind and ind["type"] == list:
                    key = obj[0][col]
                    
                    _obj = list(obj)
                    for k, v in enumerate(_obj):
                        for n in v:
                            v[n] = self.dumps_obj(v[n])

                        if col in self.obj[t] and k in self.obj[t][col]:
                            if self.compare_objs(self.obj[t][col][k], v):
                                await self.setone(t, v, wheres={key: value for key, value in v.items() if key in ind["columns"]})
                        else:
                            await self.addrows(t, [v])
                else:
                    key = obj[col]
                    _obj = dict(obj)

                    for n in _obj:
                        _obj[n] = self.dumps_obj(_obj[n])

                    if key in self.obj[t]:
                        if self.compare_objs(self.obj[t][key], _obj):
                            await self.setone(t, _obj, wheres={key: value for key, value in _obj.items() if key in ind["columns"]})
                    else:
                        await self.addrows(t, [_obj])
                
                # Обновляем SortedDict данные
                self.obj[t][key] = obj
            except Exception as e:
                print("ind", ind)
                print("col", col)
                
                print(f"Error (set_obj): {e}")
                print("|" * 50)
                print(obj)
                print("-" * 50)