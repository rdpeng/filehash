createQ <- function(filename) {
        dbCreate(filename, "DB1")
        metaname <- paste(filename, "q", sep = ".")
        dbCreate(metaname, "DB1")
        qdb <- dbInit(filename, "DB1")
        meta <- dbInit(metaname, "DB1")
        dbInsert(meta, "head", NULL)
        list(qdb = qdb, meta = meta, name = filename)
}

initQ <- function(filename) {
        list(qdb = dbInit(filename, "DB1"),
             meta = dbInit(paste(filename, "q", sep = ".")),
             name = filename)
}

createQlock <- function(dbl) {
        lockfile <- paste(dbl$name, "qlock", sep = ".")        
        status <- .Call("lock_file", lockfile)
        isTRUE(status >= 0)
}

deleteQlock <- function(dbl) {
        lockfile <- paste(dbl$name, "qlock", sep = ".")
        file.remove(lockfile)
}

putQ <- function(dbl, vals) {
        if(!createQlock(dbl))
                stop("cannot create lock file")
        on.exit(deleteQlock(dbl))

        len <- length(vals)
        nextkey <- dbFetch(dbl$meta, "head")

        for(i in seq_along(vals)) {
                key <- sha1(vals[i])
                obj <- list(value = vals[i],
                            nextkey = nextkey)
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
        if(!createQlock(dbl))
                stop("cannot create lock file")
        on.exit(deleteQlock(dbl))

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
