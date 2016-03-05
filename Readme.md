# MongoDB Partial Export

## Intro
This tool can be used to dump part of a mongodb database by following a scheme defined in a yaml file. Let's take a simple version of Github data model. You want to dump all repositories of a given account, and all data recursively associated with every repo. 

```yaml
# ./data/schema.yml
- collection: accounts
  filters:
    _id: 561b8b44726f6d2490010000
- collection: repos
  dependency: accounts
  foreign_key: account_id
  filters: 
    k1: v1
    k2: v2
- collection: commits
  dependency: repos
  foreign_key: repo_id
```
## How to 
You can use this tool by running the following command :
```shell
SOURCE_DB=db1 DEST_DB=db2 go run main.go data/schema.yml
```

`db1` is the database you want to partially dump, and `db2` is the destination database. 

## Next steps 

- Handle databases URL using env variables 
- Handle database authentication 
- Add unit tests