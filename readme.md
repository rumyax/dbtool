# `D` `B` `T`
## CLI tool to copy, run SQL,<br>and connect to PostgreSQL databases

Add repo root to `PATH`.<br>
`chmod +x dbt` if `UNIX`.<br>
Create `conf.json` before using.<br>
Use `conf.template.json` as an example.<br>
By default:
* `host` is `localhost`
* `port` is `5432`
* `user` is `postgres`

***How to use:***
```bash
dbt copy --use-cache --from "dev" --to "local"
dbt run --on "local" --file "./migration.sql"
dbt connect --to "local"
```
