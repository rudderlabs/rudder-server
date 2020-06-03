- Install [wrk](https://github.com/wg/wrk)
- Generate Basic Authorization Header

```
  echo -n <your_write_key>: | openssl base64
```

- Replace the above value for base64 encoded header `<base_64_encoded_writekey_colon>` in the `post.lua` script
- Run the load test

```
wrk -t100 -c100 -d120s -s post.lua http://localhost:8080/v1/batch
```
