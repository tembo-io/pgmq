## Updating the PGMQ Extension

#### Notes
- Updates from versions before 0.14.3 are not supported, and a full re-install
is required.
- When updating from pre-`1.0.0` versions to post-`1.0.0` versions, a stop at
`1.0.0` is required. For example, if you were to update from `0.33.1` to `1.1.0`,
you first need to update from `0.33.1` to `1.0.0`, and only after `1.0.0` is
installed you can perform the update to `1.1.0`.

#### When building from source (target version post-1.3.0)
Clone the pgmq repo and checkout the desired version. For example, for `1.3.1`:

```bash
git clone git@github.com/tembo-io:pgmq
cd pgmq
git checkout v1.3.1
```

Build and install the extension:
```bash
make
make install
```

Now, connect to postgres and run:
```
ALTER EXTENSION pgmq UPDATE
```

If successful, pgmq is updated!

#### When building from source (pre-1.3.0)

If pgrx wasn't initialized yet:
```
# Remember to use the flag for your postgres major version
cargo pgrx init --6=`which pg_config`
```

Clone the pgmq repo and checkout the desired version. For example, for `1.0.0`:
```
git clone git@github.com/tembo-io:pgmq
cd pgmq
git checkout v1.0.0
```

Then, compile and install the extension:
```
cargo pgrx install --release
```

Now, connect to postgres and run:
```
ALTER EXTENSION pgmq UPDATE
```

If successful, pgmq is updated!
