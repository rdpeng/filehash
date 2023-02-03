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
######################################################################
## Class 'filehash'

#' Filehash Class
#' 
#' These functions form the interface for a simple file-based key-value database (i.e. hash table).
#' 
#' @details Objects can be created by calls of the form \code{new("filehash", ...)}.
#' 
#' @slot name Object of class \code{"character"}, name of the database.
#' 
#' @useDynLib filehash,.registration = TRUE, .fixes = "C_"
#' @import methods
#' @exportClass filehash
setClass("filehash", representation(name = "character"))

setValidity("filehash", function(object) {
    if(length(object@name) == 0)
        "database name has length 0"
    else
        TRUE
})

setGeneric("dbName", function(db) standardGeneric("dbName"))

setMethod("dbName", "filehash", function(db) db@name)

#' @exportMethod show
#' @param object a filehash object
#' @describeIn filehash Print a filehash object
setMethod("show", "filehash",
          function(object) {
              if(length(object@name) == 0)
                  stop("database does not have a name")
              cat(gettextf("'%s' database '%s'\n", as.character(class(object)),
                           object@name))
          })

######################################################################

#' Register Database Format
#' 
#' @param name character, name of database format
#' @param funlist list of functions for creating and initializing a database format
#' @export
registerFormatDB <- function(name, funlist) {
    if(!all(c("initialize", "create") %in% names(funlist)))
        stop("need both 'initialize' and 'create' functions in 'funlist'")
    r <- list(list(create = funlist[["create"]],
                   initialize = funlist[["initialize"]]))
    names(r) <- name
    do.call("filehashFormats", r)
    TRUE
}

#' List and register filehash formats
#' 
#' List and register filehash backend database formats.
#'
#' @param \dots list of functions for registering a new database format
#' 
#' @details \code{filehashFormats} can be used to register new filehash backend database formats.  \code{filehashFormats} called with no arguments lists information on available formats
#' @return A list containing information on the available filehash formats
#' @export
filehashFormats <- function(...) {
    args <- list(...)
    n <- names(args)

    for(n in names(args)) 
        assign(n, args[[n]], .filehashFormats)
    current <- as.list(.filehashFormats)

    if(length(args) == 0)
        current
    else
    invisible(current)
}

######################################################################
## Create necessary database files.  On successful creation, return
## TRUE.  If the database already exists, don't do anything but return
## TRUE (and print a message).  If there's any other strange
## condition, return FALSE.

dbStartup <- function(dbName, type, action = c("initialize", "create")) {
    action <- match.arg(action)
    validFormat <- type %in% names(filehashFormats())
    
    if(!validFormat) 
        stop(gettextf("'%s' not a valid database format", type))
    formatList <- filehashFormats()[[type]]
    doFUN <- formatList[[action]]

    if(!is.function(doFUN))
        stop(gettextf("'%s' function for database format '%s' is not valid",
                      action, type))
    doFUN(dbName)
}    

setGeneric("dbCreate", function(db, ...) standardGeneric("dbCreate"))

#' @exportMethod dbCreate
#' @aliases dbCreate
#' @param db a filehash object
#' @param ... arguments passed to other methods
#' @describeIn filehash Create a filehash database
setMethod("dbCreate", "ANY",
          function(db, type = NULL, ...) {
              if(is.null(type))
                  type <- filehashOption()$defaultType

              dbStartup(db, type, "create")
          })
          
setGeneric("dbInit", function(db, ...) standardGeneric("dbInit"))

#' @exportMethod dbInit
#' @describeIn filehash Initialize an existing filehash database
#' @param type filehash database type
#' @aliases dbInit
setMethod("dbInit", "ANY",
          function(db, type = NULL, ...) {
              if(is.null(type))
                  type <- filehashOption()$defaultType
              dbStartup(db, type, "initialize")
          })

######################################################################
## Set options and retrieve list of options

#' Set Filehash Options
#' 
#' Set global filehash options
#' 
#' @param \dots name-value pairs for options
#' @details Currently, the only option that can be set is the default database type (\code{defaultType}) which can be "DB1", "RDS" or "DB". 
#' @return \code{filehashOptions} returns a list of current settings for all options.
#' 
#' @export
filehashOption <- function(...) {
    args <- list(...)
    n <- names(args)

    for(n in names(args)) 
        assign(n, args[[n]], .filehashOptions)
    current <- as.list(.filehashOptions)

    if(length(args) == 0)
        current
    else
        invisible(current)
}

######################################################################
## Load active bindings into an environment

#' Load a Database
#' 
#' Load entire database into an environment
#' 
#' @param db filehash database object
#' @param ... arguments passed to other methods
#' 
#' @details \code{dbLoad} loads objects in the database directly into the 
#' environment specified, like \code{load} does except with active bindings. 
#' \code{dbLoad} takes a second argument \code{env}, which is an 
#' environment, and the default for \code{env} is \code{parent.frame()}. 
#' 
#' @details The use of \code{makeActiveBinding} in \code{db2env} and 
#' \code{dbLoad} allows for potentially large databases to, at least 
#' conceptually, be used in R, as long as you don't need simultaneous access to 
#' all of the elements in the database.
#' 
setGeneric("dbLoad", function(db, ...) standardGeneric("dbLoad"))

#' @exportMethod dbLoad
#' @param env environment into which objects should be loaded
#' @param keys specific keys to be loaded (if NULL then all keys are loaded)
#' @describeIn dbLoad Method for filehash databases
setMethod("dbLoad", "filehash",
          function(db, env = parent.frame(2), keys = NULL, ...) {
              if(is.null(keys))
                  keys <- dbList(db)
              else if(!is.character(keys))
                  stop("'keys' should be a character vector")
              active <- sapply(keys, function(k) {
                  exists(k, env, inherits = FALSE)
              })
              if(any(active)) {
                  warning("keys with active/regular bindings ignored: ",
                          paste(sQuote(keys[active]), collapse = ", "))
                  keys <- keys[!active]
              }                      
              make.f <- function(k) {
                  key <- k
                  function(value) {
                      if(!missing(value)) {
                          dbInsert(db, key, value)
                          invisible(value)
                      }
                      else {
                          obj <- dbFetch(db, key)
                          obj
                      }
                  }
              }
              for(k in keys) 
                  makeActiveBinding(k, make.f(k), env)
              invisible(keys)
          })

#' @param db a filehash database object
#' @param ... arguments passed to other methods
#' 
#' @details \code{dbLazyLoad} loads objects in the database directly into the
#' environment specified, like \code{load} does except with promises. 
#' \code{dbLazyLoad} takes a second argument \code{env}, which is an 
#' environment, and the default for \code{env} is \code{parent.frame()}. 

#' @details  With \code{dbLazyLoad} database objects are "lazy-loaded" into 
#' the environment. Promises to load the objects are created in the environment 
#' specified by \code{env}.  Upon first access, those objects are copied into 
#' the environment and will from then on reside in memory.  Changes to the
#' database will not be reflected in the object residing in the environment 
#' after first access.  Conversely, changes to the object in the environment 
#' will not be reflected in the database.  This type of loading is useful for 
#' read-only databases.
#' 
#' @return dbLoad, dbLazyLoad: a character vector is returned (invisibly) containing the keys associated with the values loaded into the environment.
#' @describeIn dbLoad Lazy load a filehash database
setGeneric("dbLazyLoad", function(db, ...) standardGeneric("dbLazyLoad"))

#' @exportMethod dbLazyLoad
#' @param env environment into which objects should be loaded
#' @param keys specific keys to be loaded (if NULL then all keys are loaded)
#' @describeIn dbLoad Method for filehash databases
setMethod("dbLazyLoad", "filehash",
          function(db, env = parent.frame(2), keys = NULL, ...) {
              if(is.null(keys))
                  keys <- dbList(db)
              else if(!is.character(keys))
                  stop("'keys' should be a character vector")
              
              wrap <- function(x, env) {
                  key <- x
                  delayedAssign(x, dbFetch(db, key), environment(), env)            
              }
              for(k in keys) 
                  wrap(k, env)
              invisible(keys)
          })
          
#' @describeIn dbLoad Load active bindings into an environment and return the environment
#' 
#' @param db filehash database object
#' 
#' @return db2env: environment containing database keys
#' 
#' @details \code{db2env} loads the entire database \code{db} into an 
#' environment via calls to \code{makeActiveBinding}.  Therefore, the data 
#' themselves are not stored in the environment, but a function pointing to 
#' the data in the database is stored.  When an element of the environment is 
#' accessed, the function is called to retrieve the data from the database.  
#' If the data in the database is changed, the changes will be reflected in the 
#' environment. 
#' 
#' 
#' @seealso \code{\link{dbLoad}}, \code{\link{dbLazyLoad}}
#' 
#' @export
db2env <- function(db) {
    if(is.character(db))
        db <- dbInit(db)  ## use the default type
    env <- new.env(hash = TRUE)
    dbLoad(db, env)
    env
}

######################################################################
## Other methods

setGeneric("names")

#' @exportMethod names
#' @param x a filehash object
#' @describeIn filehash Return the keys stored in a filehash database
setMethod("names", "filehash",
          function(x) {
                  dbList(x)
          })

setGeneric("length")

#' @exportMethod length
#' @param x a filehash object
#' @describeIn filehash Return the number of objects in a filehash database
setMethod("length", "filehash",
          function(x) {
                  length(dbList(x))
          })

setGeneric("with")

#' @exportMethod with
#' @param data a filehash object
#' @param expr an R expression to be evaluated
#' @describeIn filehash Use a filehash database as an evaluation environment
setMethod("with", "filehash",
          function(data, expr, ...) {
              env <- db2env(data)
              eval(substitute(expr), env, enclos = parent.frame())
          })

setGeneric("lapply")

#' @exportMethod lapply
#' @param FUN a function to be applied
#' @param X a filehash object
#' @param keep.names Should the key names be returned in the resulting list?
#' @describeIn filehash Apply a function over the elements of a filehash database
setMethod("lapply", signature(X = "filehash"),
          function(X, FUN, ..., keep.names = TRUE) {
              FUN <- match.fun(FUN)
              keys <- dbList(X)
              rval <- vector("list", length = length(keys))
              
              for(i in seq(along = keys)) {
                  obj <- dbFetch(X, keys[i])
                  rval[[i]] <- FUN(obj, ...)
              }
              if(keep.names)
                  names(rval) <- keys
              rval
          })

######################################################################
## Database interface

#' @describeIn filehash Retrieve values associated with multiple keys (a list of those values is returned).
#' @param db a filehash object
#' @param key a character vector indicating a key (or keys) to retreive
setGeneric("dbMultiFetch", function(db, key, ...) {
        standardGeneric("dbMultiFetch")
})

#' @describeIn filehash Insert a key-value pair into the database.  If that key already exists, its associated value is overwritten. For \code{"RDS"} type databases, there is a \code{safe} option (defaults to \code{TRUE}) which allows the user to insert objects somewhat more safely (objects should not be lost in the event of an interrupt).
setGeneric("dbInsert", function(db, key, value, ...) {
        standardGeneric("dbInsert")
})

#' @describeIn filehash Retrieve the value associated with a given key.
setGeneric("dbFetch", function(db, key, ...) standardGeneric("dbFetch"))

#' @describeIn filehash Check to see if a key exists.
setGeneric("dbExists", function(db, key, ...) standardGeneric("dbExists"))

#' @describeIn filehash List all keys in the database.
setGeneric("dbList", function(db, ...) standardGeneric("dbList"))

#' @describeIn filehash The \code{dbDelete} function is for deleting elements, but for the \code{"DB1"} format all it does is remove the key from the lookup table. The actual data are still in the database (but inaccessible).  If you reinsert data for the same key, the new data are simply appended on to the end of the file.  Therefore, it's possible to have multiple copies of data lying around after a while, potentially making the database file big.  The \code{"RDS"} format does not have this problem.
setGeneric("dbDelete", function(db, key, ...) standardGeneric("dbDelete"))

#' @describeIn filehash The \code{dbReorganize} function is there for the purpose of rewriting the database to remove all of the stale entries.  Basically, this function creates a new copy of the database and then overwrites the old copy.  This function has not been tested extensively and so should be considered \emph{experimental}.  \code{dbReorganize} is not needed when using the \code{"RDS"} format.
setGeneric("dbReorganize", function(db, ...) standardGeneric("dbReorganize"))

#' @describeIn filehash Delete an entire database from the disk.
setGeneric("dbUnlink", function(db, ...) standardGeneric("dbUnlink"))

## Other
setOldClass(c("file", "connection"))
setGeneric("lockFile", function(db, ...) standardGeneric("lockFile"))

######################################################################
## Extractor/replacement

#' @exportMethod `[[`
#' @param j not used
#' @describeIn filehash Extract elements of a filehash database using character names
#' @aliases `[[,filehash,character,missing-method`
setMethod("[[", signature(x = "filehash", i = "character", j = "missing"),
          function(x, i, j) {
              dbFetch(x, i)
          })

#' @exportMethod `$`
#' @describeIn filehash Extract elements of a filehash database using character names
setMethod("$", signature(x = "filehash"),
          function(x, name) {
              dbFetch(x, name)
          })

#' @exportMethod `[[<-`
#' @param x a filehash object
#' @param i a character index
#' @param value an R object
#' @describeIn filehash Replace elements of a filehash database
setReplaceMethod("[[", signature(x = "filehash", i = "character", j = "missing"),
                 function(x, i, j, value) {
                     dbInsert(x, i, value)
                     x
                 })

#' @exportMethod `$<-`
#' @param x a filehash object
#' @param name the name of the element in the filehash database
#' @param value an R object
#' @describeIn filehash Replace elements of a filehash database
setReplaceMethod("$", signature(x = "filehash"),
                 function(x, name, value) {
                     dbInsert(x, name, value)
                     x
                 })


## Need to define these because they're not automatically caught.
## Don't need this if R >= 2.4.0.

#' @exportMethod `[`
#' @param drop should dimensions be dropped? (not used)
#' @describeIn filehash Retrieve multiple elements of a filehash database
setMethod("[", signature(x = "filehash", i = "character", j = "missing",
                         drop = "missing"),
          function(x, i , j, drop) {
                  dbMultiFetch(x, i)
          })






