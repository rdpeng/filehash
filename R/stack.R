createS <- function(filename) {
        dbCreate(filename, "DB1")
        stack <- dbInit(filename, "DB1")
        dbInsert(stack, "top", NULL)

        list(stack = stack, name = filename)
}

initS <- function(filename) {
        list(stack = dbInit(filename, "DB1"),
             name = filename)
}

lockFileS <- function(db) {
        paste(db$name, "slock", sep = ".")
}

pushS <- function(db, val) {
        if(!createLockFile(lockFileS(db)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileS(db)))

        node <- list(value = val,
                    nextkey = dbFetch(db$stack, "top"))
        topkey <- sha1(node)

        dbInsert(db$stack, topkey, node)
        dbInsert(db$stack, "top", topkey)
}

mpushS <- function(db, vals) {
        if(!createLockFile(lockFileS(db)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileS(db)))

        if(!is.list(vals))
                vals <- as.list(vals)
        topkey <- dbFetch(db$stack, "top")

        for(i in seq_along(vals)) {
                node <- list(value = vals[[i]],
                            nextkey = topkey)
                topkey <- sha1(node)

                dbInsert(db$stack, topkey, node)
                dbInsert(db$stack, "top", topkey)
        }
}

isEmptyS <- function(db) {
        h <- dbFetch(db$stack, "top")
        is.null(h)
}


topS <- function(db) {
        if(!createLockFile(lockFileS(db)))
                stop("cannot create lock file")
        tryCatch({
                h <- dbFetch(db$stack, "top")

                if(is.null(h))
                        return(NULL)
                node <- dbFetch(db$stack, h)
        }, finally = {
                deleteLockFile(lockFileS(db))
        })
        node$value
}

popS <- function(db) {
        if(!createLockFile(lockFileS(db)))
                stop("cannot create lock file")
        tryCatch({
                h <- dbFetch(db$stack, "top")

                if(is.null(h))
                        return(NULL)
                node <- dbFetch(db$stack, h)
                dbInsert(db$stack, "top", node$nextkey)
        }, finally = {
                deleteLockFile(lockFileS(db))
        })
        dbDelete(db$stack, h)
        node$value
}
