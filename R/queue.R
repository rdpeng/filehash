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

#' A Queue Class
#' 
#' A queue implementation using a \code{filehash} database
#' 
#' @details Objects can be created by calls of the form \code{new("queue", ...)} or by calling \code{createQ}.  Existing queues can be initialized with \code{initQ}.
#' 
#' @slot queue Object of class \code{"filehashDB1"}
#' @slot name Object of class \code{"character"}: the name of the queue (default is the file name in which the queue data are stored)
#' @exportClass queue
setClass("queue",
         representation(queue = "filehashDB1",
                        name = "character")
         )

#' @param filename name of queue file
#' 
#' @return \code{createQ} and \code{initQ} return a \code{queue} object
#' @export
#' @describeIn queue Create a file-based queue object
createQ <- function(filename) {
        dbCreate(filename, "DB1")
        queue <- dbInit(filename, "DB1")
        dbInsert(queue, "head", NULL)
        dbInsert(queue, "tail", NULL)

        new("queue", queue = queue, name = filename)
}

#' @export
#' @describeIn queue Intialize an existing queue object
initQ <- function(filename) {
        new("queue",
            queue = dbInit(filename, "DB1"),
            name = filename)
}

## Public

#' @describeIn queue Return (and remove) the top element of a queue
setGeneric("pop", function(db, ...) standardGeneric("pop"))

#' @describeIn queue Push an R object on to the tail of a queue
setGeneric("push", function(db, val, ...) standardGeneric("push"))

#' @describeIn queue Check if a queue is empty or not
setGeneric("isEmpty", function(db, ...) standardGeneric("isEmpty"))

#' @describeIn queue Return the top of the queue
setGeneric("top", function(db, ...) standardGeneric("top"))


#' @exportMethod show
#' @describeIn queue Print a queue object
#' @param object a queue object
setMethod("show", "queue",
          function(object) {
                  cat(gettextf("<queue: %s>\n", object@name))
                  invisible(object)
          })


################################################################################
## Methods

setMethod("lockFile", "queue",
          function(db, ...) {
                  paste(db@name, "qlock", sep = ".")
          })

#' @exportMethod push
#' @describeIn queue adds an element to the tail ("bottom") of the queue
#' @param db a queue object
#' @param val an R object to be added to the tail queue
#' @param ... arguments passed to other methods
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

#' @exportMethod isEmpty
#' @describeIn queue returns \code{TRUE}/\code{FALSE} depending on whether there are elements in the queue.
setMethod("isEmpty", "queue", function(db) {
        is.null(dbFetch(db@queue, "head"))
})

#' @exportMethod top
#' @describeIn queue returns the value of the "top" (i.e. head) of the queue; an error is signaled if the queue is empty
setMethod("top", "queue", function(db, ...) {
        createLockFile(lockFile(db))
        on.exit(deleteLockFile(lockFile(db)))

        if(isEmpty(db))
                stop("queue is empty")
        h <- dbFetch(db@queue, "head")
        node <- dbFetch(db@queue, h)
        node$value
})

#' @exportMethod pop
#' @describeIn queue returns the value of the "top" (i.e. head) of the queue and subsequently removes that element from the queue; an error is signaled if the queue is empty
#' @param db a queue object
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
