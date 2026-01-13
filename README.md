## ForSt: A Persistent Key-Value Store designed for Streaming processing

ForSt is built on top of [RocksDB](https://github.com/facebook/rocksdb) by facebook.

This code is a library that forms the core building block for a fast
key-value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it especially suitable for storing multiple terabytes of data in a
single database.

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Questions and discussions are welcome on the [Discussion](https://github.com/ververica/ForSt/discussions).

## License

ForSt is licensed under Apache 2.0 License.
