######################################################################
## Copyright (C) 2006--2008, Roger D. Peng <rpeng@jhsph.edu>
##     
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 2 of the License, or
## (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
## 02110-1301, USA
#####################################################################

######################################################################
## Class 'filehashDB1'

## Database entries
##
## File format: [key]        [nbytes data] [data]
##              serialized   serialized    raw bytes (serialized)
##

######################################################################

## 'meta' is a list with an element called 'metaEnv'.  'metaEnv' is an
## environment that contains metadata for the database.

setClass("filehashDB1",
         representation(datafile = "character",
                        meta = "list"),  ## contains 'metaEnv' element
         contains = "filehash"
         )

setValidity("filehashDB1",
            function(object) {
                    if(!file.exists(object@datafile))
                            return(gettextf("datafile '%s' does not exist",
                                            datafile))
                    if(is.null(object@meta$metaEnv))
                            return(gettextf("object is missing 'metaEnv' element"))
                    TRUE
            })

createDB1 <- function(dbName) {
        if(!hasWorkingFtell())
                stop("need working 'ftell()' to use 'DB1' format")
        if(file.exists(dbName)) {
                message(gettextf("database '%s' already exists", dbName))
                return(TRUE)
        }
        status <- file.create(dbName)

        if(!status)
                stop(gettextf("unable to create database file '%s'", dbName))
        TRUE
}

makeMetaEnv <- function(filename) {
        ## Create database map and store in environment.  Don't read map
        ## until you need it; for example, it's not needed for *writing*
        ## to the database.
        metaEnv <- new.env(parent = emptyenv())
        metaEnv$map <- NULL  ## 'NULL' indicates the map needs to be read
        metaEnv$dbfilesize <- file.info(filename)$size

        metaEnv
}

initializeDB1 <- function(dbName) {
        if(!hasWorkingFtell())
                stop("need working 'ftell()' to use DB1 format")
        dbName <- normalizePath(dbName)

        new("filehashDB1",
            datafile = dbName,
            meta = list(metaEnv = makeMetaEnv(dbName)),
            name = basename(dbName)
            )
}


readKeyMap <- function(con, map = NULL, pos = 0) {
        if(is.null(map)) {
                ## using 'hash = TRUE' is critical because it can have a major
                ## impact on performance for large databases
                map <- new.env(hash = TRUE, parent = emptyenv())
                pos <- 0
        }
        if(pos < 0)
                stop("'pos' cannot be negative")
        filename <- path.expand(summary(con)$description)
        filesize <- file.info(filename)$size

        if(pos > filesize)
                stop("'pos' cannot be greater than file size")
        .Call("read_key_map", filename, map, filesize, pos)
}

readSingleKey <- function(con, map, key) {
        start <- map[[key]]

        if(is.null(start))
                stop(gettextf("unable to obtain value for key '%s'", key))

        seek(con, start, rw = "read")
        unserialize(con)
}

readKeys <- function(con, map, keys) {
        r <- lapply(keys, function(key) readSingleKey(con, map, key))
        names(r) <- keys
        r
}

gotoEndPos <- function(con) {
        ## Move connection to the end
        seek(con, 0, "end")
        seek(con)
}

writeNullKeyValue <- function(con, key) {
        writestart <- gotoEndPos(con)

        handler <- function(cond) {
                ## Rewind the file back to where writing began and truncate at
                ## that position
                seek(con, writestart, "start", "write")
                truncate(con)
                cond
        }
        if(!createLockFile(lockFileName(con)))
                stop("cannot create lock file")
        tryCatch({
                serialize(key, con)

                len <- as.integer(-1)
                serialize(len, con)
        }, interrupt = handler, error = handler, finally = {
                flush(con)
                deleteLockFile(lockFileName(con))
        })
}

writeKeyValue <- function(con, key, value) {
        writestart <- gotoEndPos(con)

        handler <- function(cond) {
                ## Rewind the file back to where writing began and
                ## truncate at that position; this is probably a bad
                ## idea for files > 2GB
                seek(con, writestart, "start", "write")
                truncate(con)
                cond
        }
        if(!createLockFile(lockFileName(con)))
                stop("cannot create lock file")
        tryCatch({
                serialize(key, con)

                byteData <- serialize(value, NULL)
                len <- length(byteData)
                serialize(len, con)

                writeBin(byteData, con)
        }, interrupt = handler, error = handler, finally = {
                flush(con)
                deleteLockFile(lockFileName(con))
        })
}

lockFileName <- function(con) {
        ## Use 3 underscores for lock file
        sprintf("%s___LOCK", summary(con)$description)
}

createLockFile <- function(name) {
        status <- .Call("lock_file", name)
        isTRUE(status >= 0)
}

deleteLockFile <- function(name) {
        file.remove(name)
}

######################################################################
## Internal utilities

filesize <- gotoEndPos

setGeneric("checkMap", function(db, ...) standardGeneric("checkMap"))

setMethod("checkMap", "filehashDB1",
          function(db, filecon, ...) {
                  old.size <- get("dbfilesize", db@meta$metaEnv)
                  cur.size <- tryCatch({
                          filesize(filecon)
                  }, error = function(err) {
                          old.size
                  })
                  size.change <- old.size != cur.size
                  map.orig <- getMap(db)

                  map <- if(is.null(map.orig))
                          readKeyMap(filecon)
                  else if(size.change)
                          readKeyMap(filecon, map.orig, old.size)
                  else
                          map.orig
                  if(!identical(map, map.orig)) {
                          assign("map", map, db@meta$metaEnv)
                          assign("dbfilesize", cur.size, db@meta$metaEnv)
                  }
                  invisible(db)
          })


setGeneric("getMap", function(db) standardGeneric("getMap"))

setMethod("getMap", "filehashDB1",
          function(db) {
                  get("map", db@meta$metaEnv)
          })

######################################################################
## Interface functions

openDBConn <- function(filename, mode) {
        filecon <- try({
                file(filename, mode)
        }, silent = TRUE)

        if(inherits(filecon, "try-error"))
                stop("unable to open connection to database")
        filecon
}

setMethod("dbInsert",
          signature(db = "filehashDB1", key = "character", value = "ANY"),
          function(db, key, value, ...) {
                  filecon <- openDBConn(db@datafile, "ab")
                  on.exit(close(filecon))
                  writeKeyValue(filecon, key, value)
          })

setMethod("dbFetch",
          signature(db = "filehashDB1", key = "character"),
          function(db, key, ...) {
                  filecon <- openDBConn(db@datafile, "rb")
                  on.exit(close(filecon))

                  checkMap(db, filecon)
                  map <- getMap(db)

                  r <- readKeys(filecon, map, key[1])
                  r[[1]]
          })

setMethod("dbMultiFetch",
          signature(db = "filehashDB1", key = "character"),
          function(db, key, ...) {
                  filecon <- openDBConn(db@datafile, "rb")
                  on.exit(close(filecon))

                  checkMap(db, filecon)
                  map <- getMap(db)

                  readKeys(filecon, map, key)
          })

setMethod("[", signature(x = "filehashDB1", i = "character", j = "missing",
                         drop = "missing"),
          function(x, i , j, drop) {
                  dbMultiFetch(x, i)
          })

setMethod("dbExists", signature(db = "filehashDB1", key = "character"),
          function(db, key, ...) {
                  dbkeys <- dbList(db)
                  key %in% dbkeys
          })

setMethod("dbList", "filehashDB1",
          function(db, ...) {
                  filecon <- openDBConn(db@datafile, "rb")
                  on.exit(close(filecon))

                  checkMap(db, filecon)
                  map <- getMap(db)

                  if(length(map) == 0)
                          character(0)
                  else {
                          keys <- as.list(map, all.names = TRUE)
                          use <- !sapply(keys, is.null)
                          names(keys[use])
                  }
          })

setMethod("dbDelete", signature(db = "filehashDB1", key = "character"),
          function(db, key, ...) {
                  filecon <- openDBConn(db@datafile, "ab")
                  on.exit(close(filecon))

                  writeNullKeyValue(filecon, key)
          })

setMethod("dbUnlink", "filehashDB1",
          function(db, ...) {
                  file.remove(db@datafile)
          })

reorganizeDB <- function(db, ...) {
        datafile <- db@datafile

        ## Find a temporary file name
        tempdata <- paste(datafile, "Tmp", sep = "")
        i <- 0
        while(file.exists(tempdata)) {
                i <- i + 1
                tempdata <- paste(datafile, "Tmp", i, sep = "")
        }
        if(!dbCreate(tempdata, type = "DB1")) {
                warning("could not create temporary database")
                return(FALSE)
        }
        on.exit(file.remove(tempdata))

        tempdb <- dbInit(tempdata, type = "DB1")
        keys <- dbList(db)

        ## Copy all keys to temporary database
        nkeys <- length(keys)
        cat("Reorganizing database: ")

        for(i in seq_along(keys)) {
                key <- keys[i]
                msg <- sprintf("%d%% (%d/%d)", round (100 * i / nkeys),
                               i, nkeys)
                cat(msg)

                dbInsert(tempdb, key, dbFetch(db, key))

                back <- paste(rep("\b", nchar(msg)), collapse = "")
                cat(back)
        }
        cat("\n")
        status <- file.rename(tempdata, datafile)

        if(!isTRUE(status)) {
                on.exit()
                warning("temporary database could not be renamed and is left in ",
                        tempdata)
                return(FALSE)
        }
        on.exit()
        message("Finished; reload database with 'dbInit'")
        TRUE
}

setMethod("dbReorganize", "filehashDB1", reorganizeDB)


################################################################################
## Test system's ftell()

hasWorkingFtell <- function() {
        tfile <- tempfile()
        con <- file(tfile, "wb")

        tryCatch({
                bytes <- raw(10)
                begin <- seek(con)

                if(begin != 0)
                        return(FALSE)
                writeBin(bytes, con)
                end <- seek(con)
                offset <- end - begin
                isTRUE(offset == 10)
        }, finally = {
                close(con)
                unlink(tfile)
        })
}

######################################################################


