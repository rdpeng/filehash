setClass("queue",
         representation(queue = "filehashDB1",
                        name = "character")
         )

createQ <- function(filename) {
        dbCreate(filename, "DB1")
        queue <- dbInit(filename, "DB1")
        dbInsert(queue, "head", NULL)
        dbInsert(queue, "tail", NULL)

        new("queue", queue = queue, name = filename)
}

initQ <- function(filename) {
        new("queue",
            queue = dbInit(filename, "DB1"),
            name = filename)
}

setGeneric("lockFile", function(db, ...) standardGeneric("lockFile"))

## Public
setGeneric("pop", function(db, ...) standardGeneric("pop"))
setGeneric("push", function(db, val, ...) standardGeneric("push"))
setGeneric("isEmpty", function(db, ...) standardGeneric("isEmpty"))
setGeneric("top", function(db, ...) standardGeneric("top"))


################################################################################
## Methods

setMethod("lockFile", "queue",
          function(db, ...) {
                  paste(db@name, "qlock", sep = ".")
          })

setMethod("push", c("queue", "ANY"), function(db, val, ...) {
        node <- list(value = val,
                     nextkey = NULL)
        key <- sha1(node)

        if(!createLockFile(lockFile(db)))
                stop("cannot create lock file")
        on.exit(deleteLockFile(lockFile(db)))

        dbInsert(db@queue, key, node)
        h <- dbFetch(db@queue, "head")

        if(is.null(h)) {
                dbInsert(db@queue, "head", key)
                dbInsert(db@queue, "tail", key)
        }
        else {
                tl <- dbFetch(db@queue, "tail")
                oldtail <- dbFetch(db@queue, tl)
                oldtail$nextkey <- key
                dbInsert(db@queue, tl, oldtail)
                dbInsert(db@queue, "tail", key)
        }
})

setMethod("isEmpty", "queue", function(db) {
        is.null(dbFetch(db@queue, "head"))
})

setMethod("top", "queue", function(db, ...) {
        if(!createLockFile(lockFile(db)))
                stop("cannot create lock file")
        tryCatch({
                h <- dbFetch(db@queue, "head")

                if(is.null(h))
                        return(NULL)
                node <- dbFetch(db@queue, h)
        }, finally = {
                deleteLockFile(lockFile(db))
        })
        node$value
}

setMethod("pop", "queue", function(db, ...) {
        if(!createLockFile(lockFile(db)))
                stop("cannot create lock file")
        tryCatch({
                h <- dbFetch(db@queue, "head")

                if(is.null(h))
                        return(NULL)
                node <- dbFetch(db@queue, h)
                dbInsert(db@queue, "head", node$nextkey)
        }, finally = {
                deleteLockFile(lockFile(db))
        })
        dbDelete(db@queue, h)
        node$value
}
