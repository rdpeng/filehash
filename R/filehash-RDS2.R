######################################################################
## Copyright (C) 2013, Gregory Jefferis <jefferis@gmail.com>
##                     Roger D. Peng <rpeng@jhsph.edu>
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

################################################################################
## Class 'filehashRDS2'

setClass("filehashRDS2",
         representation(dir = "character", objects = "character"),
         contains = "filehashRDS"
         )

setValidity("filehashRDS2",
            function(object) {
                    if(length(object@dir) != 1)
                            return("only one directory should be set in 'dir'")
                    if(!file.exists(object@dir))
                            return(gettextf("directory '%s' does not exist",
                                            object@dir))
                    TRUE
            })

createRDS2 <- createRDS

initializeRDS2 <- function(dbName) {
        ## Trailing '/' causes a problem in Windows?
        dbName <- sub("/$", "", dbName, perl = TRUE)
        new("filehashRDS2", dir = normalizePath(dbName),
            name = basename(dbName))
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
setMethod("objectFile", signature(db = "filehashRDS2", key = "character"),
          function(db, key) {
                  sha1.key <- sha1(key)
                  # use first two letters of sha1 as subdir (git style)
                  subdir <- substr(sha1.key,1,2)
                  file.path(db@dir, subdir, mangleName(key))
          })

# Function to rescan database directory
setMethod("dbReorganize", "filehashRDS2",
    function(db, ...) {
              ## update in memory list of objects in database
              fileList <- dir(db@dir, recursive=TRUE)
              db@objects<-structure(fileList,
                      .Names=unMangleName(basename(fileList)))
    })

################################################################################
## Interface functions

setMethod("dbInsert",
          signature(db = "filehashRDS2", key = "character", value = "ANY"),
          function(db, key, value, safe = TRUE, ...) {
                  if(!safe) message("RDS2: Ignoring safe=FALSE")
                  of <- objectFile(db, key)
                  od <- dirname(of)

                  # write to temporary file within database directory
                  # this will be on same filesystem as main database
                  # allowing file.rename to be used
                  writefile <- tempfile(pattern='.RDS2tmp',tmpdir=file.path(db@dir))
                  con <- gzfile(writefile, "wb")

                  writestatus <- tryCatch({
                          serialize(value, con)
                  }, condition = function(cond) {
                          cond
                  }, finally = {
                          close(con)
                  })
                  if(inherits(writestatus, "condition"))
                          stop(gettextf("unable to write object '%s'", key))
                  
                  # make the enclosing directory
                  if(!file.exists(od)) {
                          mkdirstatus=dir.create(od)
                          if(!mkdirstatus){
                                  stop(gettextf("unable to make subdir '%s'", od))
                          }
                  }
                  # now copy the temporary file
                  cpstatus <- file.rename(writefile, of)

                  if(!cpstatus){
                          # need to remove temporary file explicitly 
                          rmstatus <- file.remove(writefile)
                          if(!rmstatus)
                                  warning("unable to remove temporary file")

                          stop(gettextf("unable to insert object '%s'", key))
                  }

                  rval=invisible(cpstatus)

                  # update object list
                  objlist <- db@objects
                  # this looks after case when we are re-inserting on same key
                  objlist[key] <- of
                  db@objects <- objlist
                  return(rval)
          })

setMethod("dbFetch", signature(db = "filehashRDS2", key = "character"),
          function(db, key, ...) {
                  ## Create filename from key
                  ofile <- objectFile(db, key)

                  ## Open connection
                  con <- tryCatch({
                          gzfile(ofile, "rb")
                  }, condition = function(cond) {
                          cond
                  })
                  if(inherits(con, "condition")) 
                          stop(gettextf("unable to obtain value for key '%s'",
                                        key))
                  on.exit(close(con))

                  ## Read data
                  val <- unserialize(con)
                  val
          })

setMethod("dbMultiFetch",
          signature(db = "filehashRDS2", key = "character"),
          function(db, key, ...) {
                  r <- lapply(key, function(k) dbFetch(db, k))
                  names(r) <- key
                  r
          })

setMethod("dbExists", signature(db = "filehashRDS2", key = "character"),
          function(db, key, ...) {
                  key %in% dbList(db)
          })

setMethod("dbList", "filehashRDS2",
          function(db, ...) {
                  ## list all keys/files in the database
                  names(db@objects)
          })

setMethod("dbDelete", signature(db = "filehashRDS2", key = "character"),
          function(db, key, ...) {
                  ofile <- objectFile(db, key)
                  
                  # remove the key from the object list
                  db@objects<-db@objects[names(db@objects)!=key]

                  ## remove/delete the file
                  status <- file.remove(ofile)
                  invisible(isTRUE(status))
          })

setMethod("dbUnlink", "filehashRDS2",
          function(db, ...) {
                  ## delete the entire database directory
                  d <- db@dir
                  status <- unlink(d, recursive = TRUE)
                  invisible(status)
          })

