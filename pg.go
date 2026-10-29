package pgme

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

type Database struct {
	Host        string        `json:"host"`
	Port        int           `json:"port"`
	Name        string        `json:"name"`
	UserName    string        `json:"username"`
	Password    string        `json:"password"`
	Channel     string        `json:"channel"`
	Timeout     time.Duration `json:"timeout"`
	Limit       int           `json:"limit"`
	pool        *pgxpool.Pool
	channelConn *pgxpool.Conn
}

func (db *Database) InitPool() error {
	if config, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s", db.UserName, db.Password, db.Host, db.Port, db.Name)); err == nil {
		config.ConnConfig.OnNotice = db.handleNotice
		if pool, err := pgxpool.NewWithConfig(context.Background(), config); err == nil {
			log.WithFields(log.Fields{
				"pool": fmt.Sprintf("%p", pool),
			}).Debug("Соединение")
			db.pool = pool
			return nil
		} else {
			return err
		}
	} else {
		log.Fatal(err)
		return err
	}
}

func (db *Database) NewConnection(ctx context.Context) (*pgxpool.Conn, error) {
	if conn, err := db.pool.Acquire(ctx); err == nil {
		log.WithFields(log.Fields{
			"connection": fmt.Sprintf("%p", conn),
		}).Debug("Соединение открыто")
		return conn, err
	} else {
		return nil, err
	}
}

func (db *Database) ClosePool() {
	db.pool.Close()
	log.WithFields(log.Fields{
		"pool": fmt.Sprintf("%p", db.pool),
	}).Debug("Закрыт")
}

func (db *Database) Disconnect(conn *pgxpool.Conn) {
	conn.Release()
	log.WithFields(log.Fields{
		"connection": fmt.Sprintf("%p", conn),
	}).Debug("Соединение закрыто")
}

func (db *Database) WaitChannel(ctx context.Context) {
	if conn, err := db.NewConnection(ctx); err == nil {
		defer db.Disconnect(conn)
		if _, err := conn.Exec(ctx, fmt.Sprintf("LISTEN \"%s\"", db.Channel)); err == nil {
			// Выполнится до Disconnect: соединение вернётся в пул уже без подписки
			defer db.unlisten(conn)
			// Цикл
			for {
				log.WithFields(log.Fields{
					"channel": db.Channel,
					"timeout": db.Timeout * time.Second,
				}).Info("Ожидание")
				timeoutCtx, cancel := context.WithTimeout(ctx, db.Timeout*time.Second)
				if notification, err := conn.Conn().WaitForNotification(timeoutCtx); err == nil {
					cancel()
					log.WithFields(log.Fields{
						"notify": notification,
					}).Info("Notify")
					break
				} else {
					cancel()
					if strings.Contains(err.Error(), "timeout") {
						break
					} else {
						log.Fatal(err)
					}
				}
				time.Sleep(1 * time.Second)
			}
		} else {
			log.Fatal(err)
		}
	} else {
		log.Fatal(err)
	}
}

// unlisten снимает подписку перед возвратом соединения в пул и выбрасывает
// уведомления, которые pgx успел положить в буфер соединения. Иначе соединение
// из пула продолжало бы копить NOTIFY во время обычных запросов, а следующий
// WaitChannel на нём сразу получал бы устаревшее уведомление.
func (db *Database) unlisten(conn *pgxpool.Conn) {
	ctx := context.Background()
	if _, err := conn.Exec(ctx, fmt.Sprintf("UNLISTEN \"%s\"", db.Channel)); err != nil {
		log.WithFields(log.Fields{
			"connection": fmt.Sprintf("%p", conn),
		}).Error(err)
		// Состояние соединения неизвестно: закрываем, пул выбросит его при Release
		_ = conn.Conn().Close(ctx)
		return
	}
	// pgx сначала отдаёт уведомления из буфера, а с уже отменённым контекстом
	// сразу возвращает ошибку, не трогая соединение
	done, cancel := context.WithCancel(ctx)
	cancel()
	dropped := 0
	for {
		if _, err := conn.Conn().WaitForNotification(done); err != nil {
			break
		}
		dropped++
	}
	log.WithFields(log.Fields{
		"channel": db.Channel,
		"dropped": dropped,
	}).Debug("UNLISTEN")
}

func (db *Database) handleNotice(_ *pgconn.PgConn, notice *pgconn.Notice) {
	if notice.Message != "" && notice.Message != "null" {
		log.Warn(notice.Message)
	}
}
