# futon

Google Drive on FUSE

* Right now, and probably forever, read only.
* Caches everything it fetches from Drive in a bolt db.
* Throws out cached files when the sum of cached blocks overflows a max cache size.

## Usage

```
go get github.com/zond/futon
futon MOUNTPOINT
```

## Thank you

* https://github.com/bazillion/fuse
* https://github.com/google/google-api-go-client
* https://github.com/boltdb/bolt
