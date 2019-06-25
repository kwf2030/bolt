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

type DBWrapper struct {
  *bbolt.DB
}

func OpenWith(path string, buckets ...string) (*DBWrapper, error) {
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
  return &DBWrapper{DB: db}, nil
}

func (w *DBWrapper) QueryAndUpdateV(bucket, k []byte, f func([]byte) ([]byte, error)) error {
  if bucket == nil || k == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.Update(func(tx *bbolt.Tx) error {
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

func (w *DBWrapper) QueryAndUpdateVPrefix(bucket, prefix []byte, f func([]byte) ([]byte, error)) error {
  if bucket == nil || prefix == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.Update(func(tx *bbolt.Tx) error {
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

func (w *DBWrapper) UpdateV(bucket, k, v []byte) error {
  if bucket == nil || k == nil || v == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return b.Put(k, v)
  })
}

func (w *DBWrapper) UpdateB(bucket []byte, f func(*bbolt.Bucket) error) error {
  if bucket == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.Update(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return f(b)
  })
}

func (w *DBWrapper) Update(f func(*bbolt.Tx) error) error {
  if f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.Update(f)
}

func (w *DBWrapper) Get(bucket, k []byte) []byte {
  if bucket == nil || k == nil {
    return nil
  }
  var ret []byte
  e := w.DB.View(func(tx *bbolt.Tx) error {
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

func (w *DBWrapper) QueryV(bucket, k []byte, f func([]byte) error) error {
  if bucket == nil || k == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.View(func(tx *bbolt.Tx) error {
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

func (w *DBWrapper) QueryB(bucket []byte, f func(*bbolt.Bucket) error) error {
  if bucket == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return f(b)
  })
}

func (w *DBWrapper) Query(f func(*bbolt.Tx) error) error {
  if f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.View(f)
}

func (w *DBWrapper) EachKV(bucket []byte, f func([]byte, []byte) error) error {
  if bucket == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.View(func(tx *bbolt.Tx) error {
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

func (w *DBWrapper) EachKVPrefix(bucket, prefix []byte, f func([]byte, []byte) error) error {
  if bucket == nil || prefix == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.View(func(tx *bbolt.Tx) error {
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

func (w *DBWrapper) EachB(bucket []byte, f func(*bbolt.Bucket) error) error {
  if bucket == nil || f == nil {
    return base.ErrInvalidArgument
  }
  return w.DB.View(func(tx *bbolt.Tx) error {
    b := tx.Bucket(bucket)
    if b == nil {
      return ErrBucketNotFound
    }
    return f(b)
  })
}

func (w *DBWrapper) CountKV(bucket []byte) (int, error) {
  if bucket == nil {
    return 0, base.ErrInvalidArgument
  }
  n := 0
  e := w.DB.View(func(tx *bbolt.Tx) error {
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

func (w *DBWrapper) CountKVPrefix(bucket, prefix []byte) (int, error) {
  if bucket == nil {
    return 0, base.ErrInvalidArgument
  }
  n := 0
  e := w.DB.View(func(tx *bbolt.Tx) error {
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
