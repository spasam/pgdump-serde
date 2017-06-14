# Hive pg_dump format support

This SerDe adds support for reading pg_dump ouput (data only)

## Using

```
hive> add jar path/to/pgdump-serde-1.0.0-1.2.0-all.jar;

hive> create table my_table(a integer, b string, ...)
  row format serde 'com.pasam.hive.serde.pg.PgDumpSerDe'
  stored as textfile
;
```

## Files

* [pgdump-serde-1.0.0-1.2.0-all.jar](https://github.com/spasam/pgdump-serde/releases/download/1.0.0/pgdump-serde-1.0.0-1.2.0-all.jar)


## Building

Run `mvn package` to build

### Eclipse support

Run `mvn eclipse:eclipse` to generate `.project` and `.classpath` files for eclipse

## License

pgdump-serde is open source and licensed under the [Apache 2 License](http://www.apache.org/licenses/LICENSE-2.0.html)
