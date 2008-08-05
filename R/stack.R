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

pushS <- function(db, vals) {
        if(!createLockFile(lockFileS(db)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileS(db)))

        if(!is.list(vals))
                vals <- as.list(vals)
        len <- length(vals)
        nextkey <- dbFetch(db$stack, "top")

        for(i in seq_along(vals)) {
                obj <- list(value = vals[[i]],
                            nextkey = nextkey)
                key <- sha1(obj)

                ## These two are critical and need to be protected
                dbInsert(db$stack, key, obj)
                dbInsert(db$stack, "top", key)

                nextkey <- key
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

                if(!is.null(h))
                        return(NULL)
                obj <- dbFetch(db$stack, h)
        }, finally = {
                deleteLockFile(lockFileS(db))
        })
        obj$value
}

popS <- function(db) {
        if(!createLockFile(lockFileS(db)))
                stop("cannot create lock file")
        tryCatch({
                h <- dbFetch(db$stack, "top")

                if(!length(h))
                        return(NULL)
                obj <- dbFetch(db$stack, h)

                ## These two are critical and need to be protected
                dbInsert(db$stack, "top", obj$nextkey)
                dbDelete(db$stack, h)
        }, finally = {
                deleteLockFile(lockFileS(db))
        })
        obj$value
}
