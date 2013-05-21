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
         representation(dir = "character", objects = "environment"),
         contains = "filehashRDS"
         )

createRDS2 <- createRDS

initializeRDS2 <- function(dbName) {
        ## Trailing '/' causes a problem in Windows?
        dbName <- sub("/$", "", dbName, perl = TRUE)
        dbDir <- normalizePath(dbName)
        objenv <- list2env(as.list(dbObjList(dbDir)),hash=TRUE)
        new("filehashRDS2", dir = dbDir, name = basename(dbName),
                objects=objenv)
}

## Function for mapping a key to a path on the filesystem
setMethod("objectFile", signature(db = "filehashRDS2", key = "character"),
          function(db, key) {
                  if(dbExists(db,key))
                        return(get(key,envir=db@objects,inherits=FALSE))
                                
                  sha1.key <- sha1(key)
                  # use first two letters of sha1 as subdir (git style)
                  subdir <- substr(sha1.key,1,2)
                  file.path(db@dir, subdir, mangleName(key))
          })

# quick function to scan the database directory
dbObjList<-function(dbDir){
        fileList <- dir(dbDir, recursive=TRUE,full.names=TRUE)
        structure(fileList, .Names=unMangleName(basename(fileList)))
}
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
                  assign(key, of, env=db@objects)
                  return(rval)
          })

setMethod("dbList", "filehashRDS2",
          function(db, ...) {
                  ## list all keys/files in the database
                  ls(envir=db@objects)
          })

setMethod("dbExists", signature(db = "filehashRDS2", key = "character"),
          function(db, key, ...) {
                  exists(key, envir = db@objects, inherits = FALSE)
          })

setMethod("dbDelete", signature(db = "filehashRDS2", key = "character"),
          function(db, key, ...) {
                  ofile <- objectFile(db, key)
                  
                  # remove the key from the object list
                  rm(list=key,envir=db@objects)

                  ## remove/delete the file
                  status <- file.remove(ofile)
                  invisible(isTRUE(status))
          })

setMethod("length", "filehashRDS2",
          function(x) {
                  length(x@objects)
          })
