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

func Open(path string, buckets ...string) (*bbolt.DB, error) {
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
        if bucket == "" {
          continue
        }
        _, e := tx.CreateBucketIfNotExists([]byte(bucket))
        if e != nil {
          return e
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

func QueryAndUpdateV(db *bbolt.DB, bucket, k []byte, f func([]byte) ([]byte, error)) error {
  if db == nil || bucket == nil || k == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    old := b.Get(k)
    if old == nil {
      return ErrKeyNotFound
    }
    val, e := f(old)
    if e != nil {
      return e
    }
    if val != nil {
      return b.Put(k, val)
    }
    return nil
  })
}

func QueryAndUpdateVPrefix(db *bbolt.DB, bucket, prefix []byte, f func([]byte) ([]byte, error)) error {
  if db == nil || bucket == nil || prefix == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    var e error
    c := b.Cursor()
    for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
      if !bytes.HasPrefix(k, prefix) {
        break
      }
      var val []byte
      val, e = f(v)
      if e != nil {
        return e
      }
      if val != nil {
        e = b.Put(k, val)
        if e != nil {
          return e
        }
      }
    }
    return nil
  })
}

func UpdateV(db *bbolt.DB, bucket, k, v []byte) error {
  if db == nil || bucket == nil || k == nil || v == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return b.Put(k, v)
  })
}

func UpdateB(db *bbolt.DB, bucket []byte, f func(*bbolt.Bucket) error) error {
  if db == nil || bucket == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return f(b)
  })
}

func Update(db *bbolt.DB, f func(*bbolt.Tx) error) error {
  if db == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.Update(f)
}

func Get(db *bbolt.DB, bucket, k []byte) []byte {
  if db == nil || bucket == nil || k == nil {
    return nil
  }
  var ret []byte
  e := db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    v := b.Get(k)
    if v == nil {
      return ErrKeyNotFound
    }
    ret = make([]byte, len(v))
    copy(ret, v)
    return nil
  })
  if e != nil {
    return nil
  }
  return ret
}

func QueryV(db *bbolt.DB, bucket, k []byte, f func([]byte) error) error {
  if db == nil || bucket == nil || k == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    v := b.Get(k)
    if v == nil {
      return ErrKeyNotFound
    }
    return f(v)
  })
}

func QueryB(db *bbolt.DB, bucket []byte, f func(*bbolt.Bucket) error) error {
  if db == nil || bucket == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return f(b)
  })
}

func Query(db *bbolt.DB, f func(*bbolt.Tx) error) error {
  if db == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.View(f)
}

func EachKV(db *bbolt.DB, bucket []byte, f func([]byte, []byte) error) error {
  if db == nil || bucket == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    var e error
    c := b.Cursor()
    for k, v := c.First(); k != nil; k, v = c.Next() {
      if e = f(k, v); e != nil {
        return e
      }
    }
    return nil
  })
}

func EachKVPrefix(db *bbolt.DB, bucket, prefix []byte, f func([]byte, []byte) error) error {
  if db == nil || bucket == nil || prefix == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    var e error
    c := b.Cursor()
    for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
      if !bytes.HasPrefix(k, prefix) {
        break
      }
      if e = f(k, v); e != nil {
        return e
      }
    }
    return nil
  })
}

func EachB(db *bbolt.DB, bucket []byte, f func(*bbolt.Bucket) error) error {
  if db == nil || bucket == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return f(b)
  })
}

func CountKV(db *bbolt.DB, bucket []byte) (int, error) {
  if db == nil || bucket == nil {
    return 0, base.ErrInvalidArgument
  }
  n := 0
  e := db.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    n = b.Stats().KeyN
    return nil
  })
  if e != nil {
    return 0, e
  }
  return n, nil
}

func CountKVPrefix(db *bbolt.DB, bucket, prefix []byte) (int, error) {
  if db == nil || bucket == nil {
    return 0, base.ErrInvalidArgument
  }
  n := 0
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
      n++
    }
    return nil
  })
  if e != nil {
    return 0, e
  }
  return n, nil
}
