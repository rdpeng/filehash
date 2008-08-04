createQ <- function(filename) {
        dbCreate(filename, "DB1")
        qdb <- dbInit(filename, "DB1")

        metaname <- paste(filename, "q", sep = ".")
        file.create(metaname)

        list(qdb = qdb, meta = metaname, name = filename)
}

initQ <- function(filename) {
        list(qdb = dbInit(filename, "DB1"),
             meta = paste(filename, "q", sep = "."),
             name = filename)
}

lockFileQ <- function(dbl) {
        paste(dbl$name, "qlock", sep = ".")
}

putQ <- function(dbl, vals) {
        if(!createLockFile(lockFileQ(dbl)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileQ(dbl)))

        len <- length(vals)
        nextkey <- dbFetch(dbl$meta, "head")

        for(i in seq_along(vals)) {
                key <- sha1(vals[i])
                obj <- list(value = vals[i], nextkey = nextkey)
                dbInsert(dbl$qdb, key, obj)
                dbInsert(dbl$meta, "head", key)
                nextkey <- key
        }
        dbInsert(dbl$meta, "head", nextkey)
}

headQkey <- function(dbl) {
        with(dbl, dbFetch(meta, "head"))
}

popQ <- function(dbl) {
        if(!createLockFile(lockFileQ(dbl)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileQ(dbl)))

        h <- headQkey(dbl)

        if(is.null(h))
                return(NULL)
        with(dbl, {
                obj <- dbFetch(qdb, h)
                dbInsert(meta, "head", obj$nextkey)
                dbDelete(qdb, h)
                obj$value
        })
}
