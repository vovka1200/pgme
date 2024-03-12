package pgme

import (
	"context"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
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

func (db *Database) initPool() error {
	if config, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s", db.UserName, db.Password, db.Host, db.Port, db.Name)); err == nil {
		config.ConnConfig.OnNotice = db.handleNotice
		if pool, err := pgxpool.ConnectConfig(context.Background(), config); err == nil {
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

func (db *Database) newConnection(ctx context.Context) (*pgxpool.Conn, error) {
	return db.pool.Acquire(ctx)
}

func (db *Database) closePool() {
	db.pool.Close()
	log.WithFields(log.Fields{
		"pool": fmt.Sprintf("%p", db.pool),
	}).Debug("Закрыт")
}

func (db *Database) disconnect(conn *pgxpool.Conn) {
	conn.Release()
	log.WithFields(log.Fields{
		"connection": fmt.Sprintf("%p", conn),
	}).Debug("Соединение закрыто")
}

func (db *Database) waitChannel(ctx context.Context) {
	if conn, err := db.newConnection(ctx); err == nil {
		defer db.disconnect(conn)
		if _, err := conn.Exec(ctx, "LISTEN $1", db.Channel); err == nil {
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

func (db *Database) handleNotice(_ *pgconn.PgConn, notice *pgconn.Notice) {
	if notice.Message != "" && notice.Message != "null" {
		log.Warn(notice.Message)
	}
}
