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
## Class 'filehashRDS'

setClass("filehashRDS",
         representation(dir = "character"),
         contains = "filehash"
         )

setValidity("filehashRDS",
            function(object) {
                if(length(object@dir) != 1)
                    return("only one directory should be set in 'dir'")
                if(!file.exists(object@dir))
                    return(gettextf("directory '%s' does not exist", object@dir))
                TRUE
            })

createRDS <- function(dbName) {
    dir <- dbName
    
    if(!file.exists(dir))
        dir.create(dir)
    else
        message(gettextf("database '%s' already exists", dbName))
    TRUE
}

initializeRDS <- function(dbName) {
    new("filehashRDS", dir = normalizePath(dbName), name = basename(dbName))
}

## For case-insensitive file systems, objects with the same name but
## differ by capitalization might get clobbered.  `mangleName()'
## inserts a "@" before each capital letter and `unMangleName()'
## reverses the operation.

mangleName <- function(oname) {
    gsub("([A-Z])", "@\\1", oname, perl = TRUE)
}

unMangleName <- function(mname) {
    gsub("@", "", mname, fixed = TRUE)
}

## Function for mapping a key to a path on the filesystem
setGeneric("objectFile", function(db, key) standardGeneric("objectFile"))
setMethod("objectFile", signature(db = "filehashRDS", key = "character"),
          function(db, key) {
              file.path(db@dir, mangleName(key))
          })

######################################################################
## Interface functions

setMethod("dbInsert",
          signature(db = "filehashRDS", key = "character", value = "ANY"),
          function(db, key, value, ...) {
              ## open connection to a gzip compressed file
              con <- gzfile(objectFile(db, key), "wb")

              ## serialize data to connection; return TRUE on success
              tryCatch({
                  serialize(value, con)
              }, error = function(err) {
                  err
              }, interrupt = function(cond) {
                  cond
              }, finally = close(con))
          }
          )

setMethod("dbFetch", signature(db = "filehashRDS", key = "character"),
          function(db, key, ...) {
              ## create filename from key
              ofile <- objectFile(db, key)

              con <- tryCatch({
                  gzfile(ofile, "rb")
              }, error = function(cond) {
                  cond
              })
              if(inherits(con, "condition")) 
                  stop(gettextf("error obtaining value for key '%s': %s", key,
                                conditionMessage(con)))
              on.exit(close(con))
              
              val <- unserialize(con)
              val
          })

setMethod("dbExists", signature(db = "filehashRDS", key = "character"),
          function(db, key, ...) {
              key %in% dbList(db)
          })

setMethod("dbList", "filehashRDS",
          function(db, ...) {
              ## list all keys/files in the database
              fileList <- dir(db@dir, all.files = TRUE, full.names = TRUE)
              use <- !file.info(fileList)$isdir
              fileList <- basename(fileList[use])

              unMangleName(fileList)
          })

setMethod("dbDelete", signature(db = "filehashRDS", key = "character"),
          function(db, key, ...) {
              ofile <- objectFile(db, key)
              
              ## remove/delete the file
              status <- file.remove(ofile)
              isTRUE(status)
          })

setMethod("dbUnlink", "filehashRDS",
          function(db, ...) {
              ## delete the entire database directory
              d <- db@dir
              unlink(d, recursive = TRUE)
          })

