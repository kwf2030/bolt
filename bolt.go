package bolt

import (
  "bytes"
  "errors"
  "os"

  "github.com/kwf2030/commons/base"
  "go.etcd.io/bbolt"
)

var (
  ErrBucketNotFound = errors.New("bucket not found")
  ErrKeyNotFound    = errors.New("key not found")
)

func Open(path string, buckets ...[]byte) (*bbolt.DB, error) {
  if path == "" {
    return nil, base.ErrInvalidArgument
  }
  db, e := bbolt.Open(path, os.ModePerm, nil)
  if e != nil {
    return nil, e
  }
  if len(buckets) > 0 {
    e = db.Update(func(tx *bbolt.Tx) error {
      for _, bucket := range buckets {
        if len(bucket) > 0 {
          _, e := tx.CreateBucketIfNotExists(bucket)
          if e != nil {
            return e
          }
        }
      }
      return nil
    })
    if e != nil {
      return nil, e
    }
  }
  return db, nil
}

func Get(db *bbolt.DB, bucket, key []byte) []byte {
  if db == nil || len(bucket) == 0 || len(key) == 0 {
    return nil
  }
  var ret []byte
  e := db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    val := b.Get(key)
    if val == nil {
      return ErrKeyNotFound
    }
    ret = make([]byte, len(val))
    copy(ret, val)
    return nil
  })
  if e != nil {
    return nil
  }
  return ret
}

func Put(db *bbolt.DB, bucket, key, value []byte) error {
  if db == nil || len(bucket) == 0 || len(key) == 0 || len(value) == 0 {
    return base.ErrInvalidArgument
  }
  return db.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return b.Put(key, value)
  })
}

func GetWithValue(db *bbolt.DB, bucket, key []byte, fun func([]byte) error) error {
  if db == nil || len(bucket) == 0 || len(key) == 0 || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    val := b.Get(key)
    if val == nil {
      return ErrKeyNotFound
    }
    return fun(val)
  })
}

func PutWithValue(db *bbolt.DB, bucket, key []byte, fun func([]byte) ([]byte, error)) error {
  if db == nil || len(bucket) == 0 || len(key) == 0 || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    val := b.Get(key)
    if val == nil {
      return ErrKeyNotFound
    }
    newVal, e := fun(val)
    if e != nil {
      return e
    }
    if newVal != nil {
      return b.Put(key, newVal)
    }
    return nil
  })
}

func PutWithValuePrefix(db *bbolt.DB, bucket, prefix []byte, fun func([]byte) ([]byte, error)) error {
  if db == nil || len(bucket) == 0 || len(prefix) == 0 || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    c := b.Cursor()
    for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
      if !bytes.HasPrefix(k, prefix) {
        break
      }
      newVal, e := fun(v)
      if e != nil {
        return e
      }
      if newVal != nil {
        e = b.Put(k, newVal)
        if e != nil {
          return e
        }
      }
    }
    return nil
  })
}

func GetWithBucket(db *bbolt.DB, bucket []byte, fun func(*bbolt.Bucket) error) error {
  if db == nil || len(bucket) == 0 || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return fun(b)
  })
}

func PutWithBucket(db *bbolt.DB, bucket []byte, fun func(*bbolt.Bucket) error) error {
  if db == nil || len(bucket) == 0 || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return fun(b)
  })
}

func GetWithDB(db *bbolt.DB, fun func(*bbolt.Tx) error) error {
  if db == nil || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.View(fun)
}

func PutWithDB(db *bbolt.DB, fun func(*bbolt.Tx) error) error {
  if db == nil || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(fun)
}

func EachKV(db *bbolt.DB, bucket []byte, fun func([]byte, []byte) error) error {
  if db == nil || len(bucket) == 0 || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    c := b.Cursor()
    for k, v := c.First(); k != nil; k, v = c.Next() {
      if e := fun(k, v); e != nil {
        return e
      }
    }
    return nil
  })
}

func EachKVPrefix(db *bbolt.DB, bucket, prefix []byte, fun func([]byte, []byte) error) error {
  if db == nil || len(bucket) == 0 || len(prefix) == 0 || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    c := b.Cursor()
    for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
      if !bytes.HasPrefix(k, prefix) {
        break
      }
      if e := fun(k, v); e != nil {
        return e
      }
    }
    return nil
  })
}

func EachBucket(db *bbolt.DB, bucket []byte, fun func(*bbolt.Bucket) error) error {
  if db == nil || len(bucket) == 0 || fun == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return fun(b)
  })
}

func CountKV(db *bbolt.DB, bucket []byte) (int, error) {
  if db == nil || len(bucket) == 0 {
    return 0, base.ErrInvalidArgument
  }
  ret := 0
  e := db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    ret = b.Stats().KeyN
    return nil
  })
  if e != nil {
    return 0, e
  }
  return ret, nil
}

func CountKVPrefix(db *bbolt.DB, bucket, prefix []byte) (int, error) {
  if db == nil || len(bucket) == 0 || len(prefix) == 0 {
    return 0, base.ErrInvalidArgument
  }
  ret := 0
  e := db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    c := b.Cursor()
    for k, _ := c.Seek(prefix); k != nil; k, _ = c.Next() {
      if !bytes.HasPrefix(k, prefix) {
        break
      }
      ret++
    }
    return nil
  })
  if e != nil {
    return 0, e
  }
  return ret, nil
}
