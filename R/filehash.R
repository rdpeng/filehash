######################################################################
## Copyright (C) 2006, Roger D. Peng <rpeng@jhsph.edu>
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
## Class 'filehash'

setClass("filehash", representation(name = "character"))

setValidity("filehash", function(object) {
    if(length(object@name) == 0)
        "database name has length 0"
    else
        TRUE
})

setGeneric("dbName", function(db) standardGeneric("dbName"))
setMethod("dbName", "filehash", function(db) db@name)

setMethod("show", "filehash",
          function(object) {
              if(length(object@name) == 0)
                  stop("database does not have a name")
              cat(gettextf("'%s' database '%s'\n", as.character(class(object)),
                           object@name))
          })


######################################################################

registerFormatDB <- function(name, funlist) {
    if(!all(c("initialize", "create") %in% names(funlist)))
        stop("need both 'initialize' and 'create' functions in 'funlist'")
    r <- list(list(create = funlist[["create"]],
                   initialize = funlist[["initialize"]]))
    names(r) <- name
    do.call("filehashFormats", r)
    TRUE
}

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

createEmptyFile <- function(name) {
    ## If the file already exists, it is overwritten
    ## con <- file(name, "wb")
    ## close(con)
    file.create(name)
}

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

setMethod("dbCreate", "ANY",
          function(db, type = NULL, ...) {
              if(is.null(type))
                  type <- filehashOption()$defaultType

              dbStartup(db, type, "create")
              TRUE
          })
          
## dbCreate <- function(dbName, type) {
##     if(missing(type))
##         type <- filehashOption()$defaultType
##     
##     dbStartup(dbName, type, "create")
##     TRUE
## }

setGeneric("dbInit", function(db, ...) standardGeneric("dbInit"))

setMethod("dbInit", "ANY",
          function(db, type = NULL, ...) {
              if(is.null(type))
                  type <- filehashOption()$defaultType
              dbStartup(db, type, "initialize")
          })

dbInitialize <- function(dbName, type) {
    .Deprecated("dbInit")
}

## dbInit <- dbInitialize <- function(dbName, type) {
##     if(missing(type))
##         type <- filehashOption()$defaultType
##     dbStartup(dbName, type, "initialize")
## }

######################################################################
## Set options and retrieve list of options

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

setGeneric("dbLoad", function(db, ...) standardGeneric("dbLoad"))

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

setGeneric("dbLazyLoad", function(db, ...) standardGeneric("dbLazyLoad"))

setMethod("dbLazyLoad", "filehash",
          function(db, env = parent.frame(2), keys = NULL, ...) {
              if(is.null(keys))
                  keys <- dbList(db)
              else if(!is.character(keys))
                  stop("'keys' should be a character vector")
              ## expr <- quote(dbFetch(db, key))
              
              wrap <- function(x, env) {
                  key <- x
                  delayedAssign(x, dbFetch(db, key), environment(), env)            
              }
              for(k in keys) 
                  wrap(k, env)
              invisible(keys)
          })
          
## Load active bindings into an environment and return the environment

db2env <- function(db) {
    if(is.character(db))
        db <- dbInit(db)  ## use the default DB type
    env <- new.env(hash = TRUE)
    dbLoad(db, env)
    env
}

######################################################################
## Other methods

setGeneric("with")
setMethod("with", "filehash",
          function(data, expr, ...) {
              env <- db2env(data)
              eval(substitute(expr), env, enclos = parent.frame())
          })

setGeneric("lapply")
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

## setGeneric("names")
## setMethod("names", signature(x = "filehash"),
##           function(x) {
## 
##           })

######################################################################
## Database interface

setGeneric("dbMultiFetch", function(db, key, ...) standardGeneric("dbMultiFetch"))
setGeneric("dbReconnect", function(db, ...) standardGeneric("dbReconnect"))
setGeneric("dbFirst", function(db, ...) standardGeneric("dbFirst"))
setGeneric("dbNext", function(db, ...) standardGeneric("dbNext"))
setGeneric("dbInsert", function(db, key, value, ...) standardGeneric("dbInsert"))
setGeneric("dbFetch", function(db, key, ...) standardGeneric("dbFetch"))
setGeneric("dbExists", function(db, key, ...) standardGeneric("dbExists"))
setGeneric("dbList", function(db, ...) standardGeneric("dbList"))
setGeneric("dbDelete", function(db, key, ...) standardGeneric("dbDelete"))
setGeneric("dbReorganize", function(db, ...) standardGeneric("dbReorganize"))
setGeneric("dbUnlink", function(db, ...) standardGeneric("dbUnlink"))
setGeneric("dbDisconnect", function(db, ...) standardGeneric("dbDisconnect"))

######################################################################
## Extractor/replacement

setMethod("[[", signature(x = "filehash", i = "character", j = "missing"),
          function(x, i, j) {
              dbFetch(x, i)
          })

setMethod("$", signature(x = "filehash", name = "character"),
          function(x, name) {
              dbFetch(x, name)
          })

setReplaceMethod("[[", signature(x = "filehash", i = "character", j = "missing"),
                 function(x, i, j, value) {
                     dbInsert(x, i, value)
                     x
                 })

setReplaceMethod("$", signature(x = "filehash", name = "character"),
                 function(x, name, value) {
                     dbInsert(x, name, value)
                     x
                 })


## Need to define these because they're not automatically caught.
## Don't need this if R >= 2.4.0.

setReplaceMethod("[[", signature(x = "filehash", i = "numeric", j = "missing"),
                 function(x, i, j, value) {
                     stop("numeric indices not allowed")
                 })

setMethod("[[", signature(x = "filehash", i = "numeric", j = "missing"),
          function(x, i, j) {
              stop("numeric indices not allowed")
          })

setMethod("[", signature(x = "filehash", i = "ANY", j = "ANY", drop = "missing"),
          function(x, i, j, drop) {
              stop("multiple indices via '[' not allowed")
          })



######################################################################
## Miscellaneous


## 'serialize()' changed from 2.3.0 to 2.4.0 so we need this function
## for back compatibility
toBytes <- function(x) {
    if(getRversion() < package_version("2.4.0"))
        charToRaw(serialize(x, NULL))
    else
        serialize(x, NULL)
}


