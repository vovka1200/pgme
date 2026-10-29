# pgMe — PostgreSQL utils for me

Небольшая обёртка над [pgx/v5](https://github.com/jackc/pgx) и `pgxpool`:

- пул соединений, который настраивается из JSON-конфига;
- получение и возврат соединений из пула с debug-логированием;
- ожидание уведомлений `LISTEN/NOTIFY` с таймаутом;
- вывод `RAISE NOTICE` из PostgreSQL в лог как предупреждений.

Для журналирования используется [logrus](https://github.com/sirupsen/logrus).

## Установка

```bash
go get github.com/vovka1200/pgme
```

## Конфигурация

`Database` удобно встраивать в конфиг приложения и заполнять из JSON:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "name": "acf",
    "username": "validator",
    "password": "secret",
    "channel": "validator",
    "timeout": 600
  }
}
```

| Поле       | Тип    | Описание                                                                 |
|------------|--------|--------------------------------------------------------------------------|
| `host`     | string | Адрес сервера PostgreSQL                                                 |
| `port`     | int    | Порт                                                                     |
| `name`     | string | Имя базы данных                                                          |
| `username` | string | Пользователь                                                             |
| `password` | string | Пароль                                                                   |
| `channel`  | string | Канал для `LISTEN` в `WaitChannel` (регистр важен, имя берётся в кавычки) |
| `timeout`  | int    | Таймаут ожидания уведомления в `WaitChannel`, **в секундах**             |
| `limit`    | int    | Зарезервировано, сейчас не используется                                  |

Строка подключения собирается как `postgres://username:password@host:port/name`. Параметры пула берутся по умолчанию из `pgxpool`.

## Использование

### Пул и запросы

```go
import (
	"context"

	"github.com/vovka1200/pgme"
)

var db pgme.Database // заполняется из конфига

func main() {
	if err := db.InitPool(); err != nil {
		log.Fatal(err)
	}
	defer db.ClosePool()

	ctx := context.Background()
	conn, err := db.NewConnection(ctx) // pgxpool.Acquire
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect(conn) // pgxpool.Conn.Release

	rows, err := conn.Query(ctx, "SELECT ...")
	// ...
}
```

`NewConnection` и `Disconnect` — это `Acquire` и `Release` пула. Соединение обязательно нужно вернуть через `Disconnect`, иначе пул исчерпается.

### Ожидание уведомлений (LISTEN/NOTIFY)

`WaitChannel` блокируется, пока в канал `channel` не придёт уведомление или не истечёт `timeout`. Обычно её крутят в цикле: «перечитать данные → ждать изменения»:

```go
go func() {
	ctx := context.Background()
	for {
		reload()              // перечитать данные из БД
		db.WaitChannel(ctx)   // ждать NOTIFY или таймаут
	}
}()
```

Как это устроено:

1. Из пула берётся соединение, на нём выполняется `LISTEN "<channel>"`.
2. Ожидание заканчивается при первом уведомлении или по таймауту. Оба случая — обычный возврат из функции, отличить их можно только по логу (`Notify` или следующее `Ожидание`).
3. Перед возвратом соединения в пул выполняется `UNLISTEN`, а уведомления, которые pgx успел сложить в буфер соединения, выбрасываются. Без этого соединение в пуле продолжало бы копить NOTIFY во время обычных запросов (память растёт), а следующий `WaitChannel` получил бы устаревшее уведомление. Если `UNLISTEN` не удался, соединение закрывается, и пул его выбрасывает.

Что нужно учитывать:

- Уведомления, пришедшие между вызовами `WaitChannel` (пока выполняется `reload()`), теряются. Поэтому после каждого возврата нужно перечитывать данные целиком, а не полагаться на содержимое уведомления.
- Если `NOTIFY` вызывается на каждую изменённую строку, одна пачка изменений даст столько же циклов. Для массовых изменений лучше слать один `NOTIFY` на оператор (триггер `FOR EACH STATEMENT`).

Пример триггера:

```sql
CREATE OR REPLACE FUNCTION test_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('tester', TG_OP);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER queue_notify
    AFTER INSERT OR UPDATE OR DELETE ON queue
    FOR EACH STATEMENT EXECUTE FUNCTION test_notify();
```

### NOTICE из PostgreSQL

Сообщения `RAISE NOTICE` выводятся через `log.Warn`. Пустые сообщения и `null` пропускаются.

## Обработка ошибок

`WaitChannel` при любой ошибке, кроме таймаута, вызывает `log.Fatal` и завершает процесс. Предполагается, что сервис перезапустится сам, например по `restart: always` в Docker. Ошибка `InitPool` на этапе разбора строки подключения тоже приводит к `log.Fatal`.

## Логи

| Уровень | Сообщение                                 | Когда                                              |
|---------|-------------------------------------------|----------------------------------------------------|
| debug   | `Соединение`, `Соединение открыто/закрыто` | Создание пула, `NewConnection`, `Disconnect`       |
| info    | `Ожидание`                                | Начало ожидания в `WaitChannel`                    |
| info    | `Notify`                                  | Пришло уведомление                                 |
| debug   | `UNLISTEN` (`dropped`)                    | Снятие подписки; `dropped` — сколько выброшено из буфера |
| warn    | текст NOTICE                              | `RAISE NOTICE` из базы                             |

## Лицензия

См. [LICENSE](LICENSE).
