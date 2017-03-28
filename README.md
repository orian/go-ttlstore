# go-ttlstore
Simple Golang TTL store

It allow to set a Janitor which is notified about changes in store.

Provided Janitor:
 - keeps the number of items under N (keep the most recent ones)
 - deletes old items (maxAge)
