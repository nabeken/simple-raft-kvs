package main

import (
	"io/ioutil"
	"os"

	"github.com/armon/gomdb"
)

const (
	dbName       = "kvs"
	dbFlags uint = mdb.NOTLS
)

type LMDB struct {
	dir string
	env *mdb.Env
	dbi *mdb.DBI
}

func NewLMDB() (db *LMDB, err error) {
	defer func() {
		if err != nil {
			panic("NewLMDB")
		}
	}()
	env, err := mdb.NewEnv()
	if err != nil {
		return nil, err
	}

	dir, err := ioutil.TempDir("", "simple-kvs")
	if err != nil {
		return nil, err
	}

	if err := env.SetMaxDBs(mdb.DBI(1)); err != nil {
		return nil, err
	}

	if err := env.Open(dir, dbFlags, 0755); err != nil {
		return nil, err
	}

	txn, err := env.BeginTxn(nil, dbFlags|mdb.CREATE)
	if err != nil {
		txn.Abort()
		return nil, err
	}
	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &LMDB{
		dir: dir,
		env: env,
	}, nil
}

func (db *LMDB) Get(key []byte) ([]byte, error) {
	txn, err := db.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		return nil, err
	}
	defer txn.Abort()

	dbi, err := txn.DBIOpen(dbName, 0)
	if err != nil {
		return nil, mdbError(err)
	}

	cursor, err := txn.CursorOpen(dbi)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	_, val, err := cursor.Get([]byte(key), mdb.SET_KEY)
	if err != nil {
		switch err {
		case mdb.NotFound:
			return nil, ErrNotFound
		default:
			return nil, err
		}
	}
	return val, err
}

func (db *LMDB) Set(key, val []byte) error {
	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	dbi, err := txn.DBIOpen(dbName, mdb.CREATE)
	if err != nil {
		txn.Abort()
		return mdbError(err)
	}

	if err := txn.Put(dbi, key, val, 0); err != nil {
		txn.Abort()
		return err
	}

	if err := txn.Commit(); err != nil {
		txn.Abort()
		return err
	}
	return nil
}

func (db *LMDB) Del(key []byte) error {
	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	dbi, err := txn.DBIOpen(dbName, mdb.CREATE)
	if err != nil {
		txn.Abort()
		return mdbError(err)
	}

	if err := txn.Del(dbi, key, nil); err != nil {
		defer txn.Abort()
		switch err {
		case mdb.NotFound:
			return ErrNotFound
		default:
			return err
		}
	}

	if err := txn.Commit(); err != nil {
		txn.Abort()
		return err
	}
	return nil
}

func (db *LMDB) Close() {
	db.env.Close()
	os.RemoveAll(db.dir)
}

func mdbError(err error) error {
	if err != nil {
		switch err {
		case mdb.NotFound:
			return ErrNotFound
		default:
			return err
		}
	}
	return nil
}
