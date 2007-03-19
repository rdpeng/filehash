library(filehash)

## DB1 format
dbCreate("testDB", "DB1")
db <- dbInitialize("testDB", "DB1")

set.seed(10)
dbInsert(db, "x", 1:10)
dbInsert(db, "y", rnorm(100))
dbInsert(db, "z", c(TRUE, FALSE))
dbInsert(db, "a", letters)

library(datasets)
dbInsert(db, "df", airquality)
dbInsert(db, "h", list(1, 2, 3, 4, 5))

