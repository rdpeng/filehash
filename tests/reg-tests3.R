## Other tests

library(filehash)

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



