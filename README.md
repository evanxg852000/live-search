

```bash
cat movies.json | while read line
do
echo "$line"
curl --header "Content-Type: application/json" \
  --request POST \
  --data "$line" \
  http://127.0.0.1:8080/documents
done
```

```bash
curl -X POST -H "Content-Type: application/json" --data @movies.json http://127.0.0.1:8080/documents

curl -X GET http://127.0.0.1:8080/documents?query=avengers
```
