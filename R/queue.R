##########################################################################
## Copyright (C) 2006-2023, Roger D. Peng <roger.peng @ austin.utexas.edu>
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
##########################################################################

setClass("queue",
         representation(queue = "filehashDB1",
                        name = "character")
         )

setMethod("show", "queue",
          function(object) {
                  cat(gettextf("<queue: %s>\n", object@name))
                  invisible(object)
          })

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
        ## Create a new tail node
        node <- list(value = val,
                     nextkey = NULL)
        key <- sha1(node)

        createLockFile(lockFile(db))
        on.exit(deleteLockFile(lockFile(db)))

        if(isEmpty(db))
                dbInsert(db@queue, "head", key)
        else {
                ## Convert tail node to regular node
                tailkey <- dbFetch(db@queue, "tail")
                oldtail <- dbFetch(db@queue, tailkey)
                oldtail$nextkey <- key
                dbInsert(db@queue, tailkey, oldtail)
        }
        ## Insert new node and point tail to new node
        dbInsert(db@queue, key, node)
        dbInsert(db@queue, "tail", key)
})

setMethod("isEmpty", "queue", function(db) {
        is.null(dbFetch(db@queue, "head"))
})

setMethod("top", "queue", function(db, ...) {
        createLockFile(lockFile(db))
        on.exit(deleteLockFile(lockFile(db)))

        if(isEmpty(db))
                stop("queue is empty")
        h <- dbFetch(db@queue, "head")
        node <- dbFetch(db@queue, h)
        node$value
})

setMethod("pop", "queue", function(db, ...) {
        createLockFile(lockFile(db))
        on.exit(deleteLockFile(lockFile(db)))

        if(isEmpty(db))
                stop("queue is empty")
        h <- dbFetch(db@queue, "head")
        node <- dbFetch(db@queue, h)
        dbInsert(db@queue, "head", node$nextkey)
        dbDelete(db@queue, h)
        node$value
})
