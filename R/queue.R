createQ <- function(filename) {
        dbCreate(filename, "DB1")
        qdb <- dbInit(filename, "DB1")

        metaname <- paste(filename, "head", sep = ".")
        file.create(metaname)

        list(qdb = qdb, meta = metaname, name = filename)
}

initQ <- function(filename) {
        list(qdb = dbInit(filename, "DB1"),
             meta = paste(filename, "head", sep = "."),
             name = filename)
}

lockFileQ <- function(dbl) {
        paste(dbl$name, "qlock", sep = ".")
}

putQ <- function(dbl, vals) {
        if(!createLockFile(lockFileQ(dbl)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileQ(dbl)))

        if(!is.list(vals))
                vals <- as.list(vals)
        len <- length(vals)
        nextkey <- readLines(dbl$meta)

        for(i in seq_along(vals)) {
                obj <- list(value = vals[[i]], nextkey = nextkey)
                key <- sha1(obj)

                ## These two are critical and need to be protected
                writeLines(key, dbl$meta)
                dbInsert(dbl$qdb, key, obj)

                nextkey <- key
        }
        writeLines(nextkey, dbl$meta)
}

headQkey <- function(dbl) {
        with(dbl, readLines(meta))
}

popQ <- function(dbl) {
        if(!createLockFile(lockFileQ(dbl)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileQ(dbl)))

        h <- headQkey(dbl)

        if(!length(h))
                return(NULL)
        obj <- dbFetch(dbl$qdb, h)

        ## These two are critical and need to be protected
        writeLines(obj$nextkey, meta)
        dbDelete(dbl$qdb, h)

        obj$value
}
