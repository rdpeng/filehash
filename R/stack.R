createS <- function(filename) {
        dbCreate(filename, "DB1")
        sdb <- dbInit(filename, "DB1")

        metaname <- paste(filename, "head", sep = ".")
        file.create(metaname)

        list(sdb = sdb, meta = metaname, name = filename)
}

initS <- function(filename) {
        list(sdb = dbInit(filename, "DB1"),
             meta = paste(filename, "head", sep = "."),
             name = filename)
}

lockFileS <- function(dbl) {
        paste(dbl$name, "slock", sep = ".")
}

putS <- function(dbl, vals) {
        if(!createLockFile(lockFileS(dbl)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileS(dbl)))

        if(!is.list(vals))
                vals <- as.list(vals)
        len <- length(vals)
        nextkey <- readLines(dbl$meta)

        for(i in seq_along(vals)) {
                obj <- list(value = vals[[i]], nextkey = nextkey)
                key <- sha1(obj)

                ## These two are critical and need to be protected
                writeLines(key, dbl$meta)
                dbInsert(dbl$sdb, key, obj)

                nextkey <- key
        }
        writeLines(nextkey, dbl$meta)
}

headSkey <- function(dbl) {
        with(dbl, readLines(meta))
}

popS <- function(dbl) {
        if(!createLockFile(lockFileS(dbl)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFileS(dbl)))

        h <- headSkey(dbl)

        if(!length(h))
                return(NULL)
        obj <- dbFetch(dbl$sdb, h)

        ## These two are critical and need to be protected
        writeLines(obj$nextkey, dbl$meta)
        dbDelete(dbl$sdb, h)

        obj$value
}
