library(filehash)

######################################################################
## Test 'filehashRDS' class

dbCreate("mydbRDS", "RDS")
db <- dbInit("mydbRDS", "RDS")
show(db)

## Put some data into it
set.seed(1000)
dbInsert(db, "a", 1:10)
dbInsert(db, "b", rnorm(100))
dbInsert(db, "c", 100:1)
dbInsert(db, "d", runif(1000))
dbInsert(db, "other", "hello")

dbList(db)

dbExists(db, "e")
dbExists(db, "a")

env <- db2env(db)
ls(env)

env$a
env$b
env$c
str(env$d)
env$other

env$b <- rnorm(100)
mean(env$b)

env$a[1:5] <- 5:1
print(env$a)

dbDelete(db, "c")

tryCatch(print(env$c), error = function(e) print(e))
tryCatch(dbFetch(db, "c"), error = function(e) print(e))


######################################################################
## test filehashDB1 class

dbCreate("mydb", "DB1")
db <- dbInit("mydb", "DB1")

## Put some data into it
set.seed(1000)
dbInsert(db, "a", 1:10)
dbInsert(db, "b", rnorm(100))
dbInsert(db, "c", 100:1)
dbInsert(db, "d", runif(1000))
dbInsert(db, "other", "hello")

dbList(db)

env <- db2env(db)
ls(env)

env$a
env$b
env$c
str(env$d)
env$other

env$b <- rnorm(100)
mean(env$b)

env$a[1:5] <- 5:1
print(env$a)

dbDelete(db, "c")

tryCatch(print(env$c), error = function(e) print(e))
tryCatch(dbFetch(db, "c"), error = function(e) print(e))

numbers <- rnorm(100)
dbInsert(db, "numbers", numbers)
b <- dbFetch(db, "numbers")
stopifnot(all.equal(numbers, b))
stopifnot(identical(numbers, b))




######################################################################
######################################################################
## Test everything on all database formats

types <- c("DB1", "RDS")
set.seed(1000)

for(type in types) {
    cat("-----------------\n")
    cat("-----------------\n")
    cat("TESTING TYPE", type, "\n")
    cat("-----------------\n")
    cat("-----------------\n")

    name <- paste("mydb", type, sep = "")
    dbCreate(name, type)
    db <- dbInit(name, type)

    dbInsert(db, "a", 1:10)  ## integer
    dbInsert(db, "b", rnorm(100))  ## numeric
    dbInsert(db, "c", 100:1)  ## integer
    dbInsert(db, "d", runif(1000))  ## numeric
    dbInsert(db, "other", "hello")  ## character

    ## Use extractor/replacement methods
    db$list <- as.list(1:100)
    db$dataf <- data.frame(x = rnorm(2000), y = rnorm(2000), z = rnorm(2000))

    show(db)

    str(db$dataf)
    str(db$list)
    print(db$d)
    print(db$a)
    print(db$b)
    print(db$c)
    print(db$other)
        
    env <- db2env(db)
    ls(env)
    
    print(env$a)
    print(env$b)
    print(env$c)
    str(env$d)
    print(env$other)

    env$b <- rnorm(100)
    mean(env$b)
    
    env$a[1:5] <- 5:1
    print(env$a)

    with(db, print(mean(b)))

    r <- lapply(db, summary)
    str(r)

    dbDelete(db, "c")

    tryCatch(print(env$c), error = function(e) print(e))
    tryCatch(dbFetch(db, "c"), error = function(e) print(e))
    
    numbers <- rnorm(100)
    dbInsert(db, "numbers", numbers)
    b <- dbFetch(db, "numbers")
    stopifnot(all.equal(numbers, b))
    stopifnot(identical(numbers, b))
}



## Other tests

rm(list = ls())


dbCreate("testLoadingDB", "DB1")
db <- dbInit("testLoadingDB", "DB1")

set.seed(234)

db$a <- rnorm(100)
db$b <- runif(1000)

dbLoad(db)  ## 'a', 'b'
summary(a)
summary(b)

rm(list = ls())
db <- dbInit("testLoadingDB", "DB1")

dbLazyLoad(db)

summary(a)
summary(b)



################################################################################
## Check dbReorganize

dbCreate("test_reorg", "DB1")
db <- dbInit("test_reorg", "DB1")

set.seed(1000)
dbInsert(db, "a", 1)
dbInsert(db, "a", 1)
dbInsert(db, "a", 1)
dbInsert(db, "a", 1)
dbInsert(db, "b", rnorm(1000))
dbInsert(db, "b", rnorm(1000))
dbInsert(db, "b", rnorm(1000))
dbInsert(db, "b", rnorm(1000))
dbInsert(db, "c", runif(1000))
dbInsert(db, "c", runif(1000))
dbInsert(db, "c", runif(1000))
dbInsert(db, "c", runif(1000))

summary(db$b)
summary(db$c)

print(file.info(db@datafile)$size)

dbReorganize(db)

db <- dbInit("test_reorg", "DB1")

print(file.info(db@datafile)$size)

summary(db$b)
summary(db$c)
