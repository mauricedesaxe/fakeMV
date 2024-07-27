This lib helps you create materialized views in your SQLite database.

## Parameters kinda suck

Whatever the SQL query you pass to the `CreateMV` function, it will be executed every time you call the `RefreshMV` function. So you can't quite pass parameters well unless you bake them in the string.

**A simple example**: you want a materialized view for the past 7 days of a certain data set. SQLite doesn't have `interval` syntax so you'd normally have to do something like `SELECT id, amount, category, user_id FROM events date >= ?`. That won't work. You could limit the amount of rows instead, because the number can be baked into the query: `SELECT id, amount, category, user_id FROM events ORDER BY date DESC LIMIT 7`.

Take it up with whoever makes SQLite syntax to give you `interval` and `materialized views`. 