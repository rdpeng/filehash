######################################################################
## Copyright (C) 2017, Roger D. Peng <rdpeng@jhu.edu>
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

setClass("mdb_env")
setClass("filehashThor",
         representation(object = "mdb_env", path = "character"),
         contains = "filehash"
)

#####################################################################
## Helpers

## NOTE: Need to allow create/initialize to pass options

createThor <- function(dbName) {
        if(!require(thor))
                stop("the 'thor package is required to create a database in this format")
        if(file.exists(dbName)) {
                message(gettextf("database '%s' already exists", dbName))
                return(TRUE)
        }
        db <- mdb_env(dbName, create = TRUE, subdir = FALSE)
        invisible(db)
}

initializeThor <- function(dbName) {
        if(!require(thor))
                stop("the 'thor' package is required to initialize a database in this format")
        dbName <- normalizePath(dbName)
        object <- mdb_env(dbName, create = FALSE, subdir = FALSE,
                          mapsize = as.integer(2^20 * 10L))
        new("filehashThor", 
            object = object, 
            path = dbName,
            name = basename(dbName))
}


#####################################################################
## Interface

setMethod("dbInsert",
          signature(db = "filehashThor", key = "character", value = "ANY"),
          function(db, key, value, ...) {
                  txn <- db@object$begin(write = TRUE)
                  tryCatch({
                          value_raw <- serialize(value, NULL)
                          txn$put(key, value_raw)
                  }, error = function(e) {
                          txn$abort()
                          stop(e)
                  }, finally = {
                          txn$commit()
                  })
          })


setMethod("dbFetch",
          signature(db = "filehashThor", key = "character"),
          function(db, key, ...) {
                  txn <- db@object$begin(write = FALSE)
                  tryCatch({
                          value_raw <- txn$get(key)
                          obj <- unserialize(value_raw)
                  }, error = function(e) {
                          txn$abort()
                          stop(e)
                  }, finally = {
                          txn$commit()
                  })
                  obj
          })

        
setMethod("dbList",
          signature(db = "filehashThor"),
          function(db, key, ...) {
                  txn <- db@object$begin(write = FALSE)
                  tryCatch({
                          keys <- txn$list()
                  }, error = function(e) {
                          txn$abort()
                          stop(e)
                  }, finally = {
                          txn$commit()
                  })
                  keys
          })


setMethod("dbExists",
          signature(db = "filehashThor",
                    key = "character"),
          function(db, key, ...) {
                  txn <- db@object$begin(write = FALSE)
                  tryCatch({
                          status <- db@object$exists(key)
                  }, error = function(e) {
                          txn$abort()
                          stop(e)
                  }, finally = {
                          txn$commit()
                  })
                  status
          })

setMethod("dbDelete",
          signature(db = "filehashThor",
                    key = "character"),
          function(db, key, ...) {
                  txn <- db@object$begin(write = TRUE)
                  tryCatch({
                          txn$del(key)
                  }, error = function(e) {
                          txn$abort()
                          stop(e)
                  }, finally = {
                          txn$commit()
                  })
                  invisible(NULL)        
          })


setMethod("dbMultiFetch",
          signature(db = "filehashThor",
                    key = "character"),
          function(db, key, ...) {
                  txn <- db@object$begin(write = FALSE)
                  tryCatch({
                          vals <- lapply(txn$mget(key), unserialize)                        
                          names(vals) <- key
                  }, error = function(e) {
                          txn$abort()
                          stop(e)
                  }, finally = {
                          txn$commit()
                  })
                  vals
          })























