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

#' Stack Class
#' 
#' A stack implementation using a \code{filehash} database
#' 
#' @details Objects can be created by calls of the form \code{new("stack", ...)} or by calling \code{createS}.  Existing queues can be initialized with \code{initS}.
#' 
#' @slot stack Object of class \code{"filehashDB1"}
#' @slot name Object of class \code{"character"}: the name of the stack (default is the file name in which the stack data are stored)
#' 
#' @exportClass stack
setClass("stack",
         representation(stack = "filehashDB1",
                        name = "character"))

#' @exportMethod show
#' @describeIn stack Print a stack object.
#' @param object a stack object
setMethod("show", "stack",
          function(object) {
                  cat(gettextf("<stack: %s>\n", object@name))
                  invisible(object)
          })

#' @param filename name of file to store stack
#' @export
#' @describeIn stack Create a filehash Stack
#' @return a stack object
createS <- function(filename) {
        dbCreate(filename, "DB1")
        stack <- dbInit(filename, "DB1")
        dbInsert(stack, "top", NULL)

        new("stack", stack = stack, name = filename)
}

#' @describeIn stack Initialize and existing filehash stack
#' @param filename name of file where stack is stored
#' @export
initS <- function(filename) {
        new("stack",
            stack = dbInit(filename, "DB1"),
             name = filename)
}


setMethod("lockFile", "stack",
          function(db, ...) {
                  paste(db@name, "slock", sep = ".")
          })

#' @exportMethod push
#' @param db a stack object
#' @param val an R object to be added to the stack
#' @param ... arguments passed to other methods
#' @describeIn stack Push an object on to the stack
setMethod("push", c("stack", "ANY"), function(db, val, ...) {
        node <- list(value = val,
                    nextkey = dbFetch(db@stack, "top"))
        topkey <- sha1(node)

        createLockFile(lockFile(db))
        on.exit(deleteLockFile(lockFile(db)))

        dbInsert(db@stack, topkey, node)
        dbInsert(db@stack, "top", topkey)
})

#' @describeIn stack Push multiple R objects on to a stack
setGeneric("mpush", function(db, vals, ...) standardGeneric("mpush"))

#' @exportMethod mpush
#' @param vals a list of R objects
#' @describeIn stack Push a list of R objects on to the stack
setMethod("mpush", c("stack", "ANY"), function(db, vals, ...) {
        if(!is.list(vals))
                vals <- as.list(vals)
        createLockFile(lockFile(db))
        on.exit(deleteLockFile(lockFile(db)))

        topkey <- dbFetch(db@stack, "top")

        for(i in seq_along(vals)) {
                node <- list(value = vals[[i]],
                             nextkey = topkey)
                topkey <- sha1(node)

                dbInsert(db@stack, topkey, node)
                dbInsert(db@stack, "top", topkey)
        }
})

#' @exportMethod isEmpty
#' @describeIn stack Indicate whether the stack is empty or not
setMethod("isEmpty", "stack", function(db, ...) {
        h <- dbFetch(db@stack, "top")
        is.null(h)
})


#' @exportMethod top
#' @describeIn stack Return the top element of the stack
setMethod("top", "stack", function(db, ...) {
        createLockFile(lockFile(db))
        on.exit(deleteLockFile(lockFile(db)))

        if(isEmpty(db))
                stop("stack is empty")
        h <- dbFetch(db@stack, "top")
        node <- dbFetch(db@stack, h)
        node$value
})

#' @exportMethod pop
#' @describeIn stack Return the top element of the stack and remove that element from the stack
setMethod("pop", "stack", function(db, ...) {
        createLockFile(lockFile(db))
        on.exit(deleteLockFile(lockFile(db)))

        if(isEmpty(db))
                stop("stack is empty")
        h <- dbFetch(db@stack, "top")
        node <- dbFetch(db@stack, h)

        dbInsert(db@stack, "top", node$nextkey)
        dbDelete(db@stack, h)
        node$value
})
