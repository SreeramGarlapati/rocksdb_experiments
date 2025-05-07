# rocksdb_experiments

## Prerequisites
- Build RocksDB and ensure the shared libraries are available in `/Users/sreeramg/gits/rocksdb`.
- Install dependencies: snappy, lz4, zstd, bzip2 (e.g., via Homebrew).

## Running the Program

Set the `DYLD_LIBRARY_PATH` to include all required library paths:

```
DYLD_LIBRARY_PATH=/Users/sreeramg/gits/rocksdb:/opt/homebrew/opt/snappy/lib:/opt/homebrew/opt/lz4/lib:/opt/homebrew/opt/zstd/lib ./rocksdb_init <spikeType> [additional parameters]
```

### Example Commands

#### instance_open_benchmark
```
DYLD_LIBRARY_PATH=/Users/sreeramg/gits/rocksdb:/opt/homebrew/opt/snappy/lib:/opt/homebrew/opt/lz4/lib:/opt/homebrew/opt/zstd/lib ./rocksdb_init instance_open_benchmark
```

#### scaleout (10 instances)
```
DYLD_LIBRARY_PATH=/Users/sreeramg/gits/rocksdb:/opt/homebrew/opt/snappy/lib:/opt/homebrew/opt/lz4/lib:/opt/homebrew/opt/zstd/lib ./rocksdb_init scaleout 10
```

#### repeatedwrites (standard)
```
DYLD_LIBRARY_PATH=/Users/sreeramg/gits/rocksdb:/opt/homebrew/opt/snappy/lib:/opt/homebrew/opt/lz4/lib:/opt/homebrew/opt/zstd/lib ./rocksdb_init repeatedwrites standard
```

#### repeatedwrites (update_in_place)
```
DYLD_LIBRARY_PATH=/Users/sreeramg/gits/rocksdb:/opt/homebrew/opt/snappy/lib:/opt/homebrew/opt/lz4/lib:/opt/homebrew/opt/zstd/lib ./rocksdb_init repeatedwrites update_in_place
```

#### secondarymode (primary)
```
DYLD_LIBRARY_PATH=/Users/sreeramg/gits/rocksdb:/opt/homebrew/opt/snappy/lib:/opt/homebrew/opt/lz4/lib:/opt/homebrew/opt/zstd/lib ./rocksdb_init secondarymode primary
```

#### secondarymode (secondary)
```
DYLD_LIBRARY_PATH=/Users/sreeramg/gits/rocksdb:/opt/homebrew/opt/snappy/lib:/opt/homebrew/opt/lz4/lib:/opt/homebrew/opt/zstd/lib ./rocksdb_init secondarymode secondary
``` 