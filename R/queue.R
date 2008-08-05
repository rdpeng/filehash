createQ <- function(filename) {
        dbCreate(filename, "DB1")
        queue <- dbInit(filename, "DB1")
        dbInsert(queue, "head", NULL)
        dbInsert(queue, "tail", NULL)

        list(queue = queue, name = filename)
}

initQ <- function(filename) {
        list(queue = dbInit(filename, "DB1"),
             name = filename)
}

lockFileQ <- function(db) {
        paste(db$name, "qlock", sep = ".")
}

pushQ <- function(db, val) {
        if(!createLockFile(lockFileQ(db)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileQ(db)))

        node <- list(value = val,
                     nextkey = NULL)
        key <- sha1(node)
        dbInsert(db$queue, key, node)
        dbInsert(db$queue, "tail", sha1(node))

        if(is.null(dbFetch(db$queue, "head")))
                dbInsert(db$queue, "head", key)
}

isEmptyQ <- function(db) {
        is.null(dbFetch(db$queue, "head"))
}

headQ <- function(db) {
        if(!createLockFile(lockFileS(db)))
                stop("cannot create lock file")
        tryCatch({
                h <- dbFetch(db$queue, "head")
                
                if(is.null(h))
                        return(NULL)
                node <- dbFetch(db$queue, h)
        }, finally = {
                deleteLockFile(lockFileQ(db))
        })
        node$value
}

popQ <- function(db) {
        if(!createLockFile(lockFileS(db)))
                stop("cannot create lock file")
        tryCatch({
                h <- dbFetch(db$queue, "head")

                if(is.null(h))
                        return(NULL)
                node <- dbFetch(db$queue, h)
                dbInsert(db$queue, "head", node$nextkey)
        }, finally = {
                deleteLockFile(lockFileQ(db))
        })
        dbDelete(db$queue, h)
        node$value
}
