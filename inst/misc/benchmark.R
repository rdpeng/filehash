library(filehash)

nkeys <- 1000
datasize <- 200

types <- c("DB1", "RDS", "DB")
keys <- paste("key", 1:nkeys, sep = "")

for(type in types) {
    name <- paste("bench", type, sep = "")
    dbCreate(name, type)
    db <- dbInitialize(name, type)
    t1 <- system.time({
        for(i in seq(nkeys)) {
            dbInsert(db, keys[i], runif(datasize))
        }
    })
    results <- vector("list", length = nkeys)
    t2 <- system.time({
        for(i in seq(nkeys)) {
            results[[i]] <- dbFetch(db, keys[i])
        }
    })
    rm(db)
    tinit <- system.time(db2 <- dbInitialize(name, type))

    cat("Insert data (", type, "): ", paste(round(t1, 3), collapse = " "), "\n",
        sep = "")
    cat("Fetch data (", type , "): ", paste(round(t2, 3), collapse = " "), "\n",
        sep = "")
    cat("Initialize (", type, "): ", paste(round(tinit, 3), collapse = " "), "\n",
        sep = "")
    cat("\n")
    
    dbUnlink(db2)
}
