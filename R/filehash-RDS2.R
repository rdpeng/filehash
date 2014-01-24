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
         representation(dir = "character", objects = "environment",
         compression = "logical", xdr = "logical"),
         prototype(compression = FALSE, xdr = FALSE),
         contains = "filehashRDS"
         )

createRDS2 <- createRDS

initializeRDS2 <- function(dbName) {
        ## Trailing '/' causes a problem in Windows?
        dbName <- sub("/$", "", dbName, perl = TRUE)
        dbDir <- normalizePath(dbName)
        objenv <- new.env()
        dbSetObjList(objenv, dbObjListFromDisk(dbDir))
        new("filehashRDS2", dir = dbDir, name = basename(dbName),
                objects=objenv)
}

## Function for mapping a key to a path on the filesystem
setMethod("objectFile", signature(db = "filehashRDS2", key = "character"),
          function(db, key) {
                  if(dbExists(db,key))
                        return(dbValForKeys(db@objects,key))
                                
                  sha1.key <- sha1(key)
                  # use first two letters of sha1 as subdir (git style)
                  subdir <- substr(sha1.key,1,2)
                  file.path(db@dir, subdir, mangleName(key))
          })

# quick function to scan the database directory
dbObjListFromDisk<-function(dbDir){
        subdirs <- dir(dbDir,full.names=T)
        # note use of unlist in case there are any empty directories
        fileList <- unlist(sapply(subdirs,dir,full.names=T))
        if(!length(fileList)) return(NULL)
        structure(fileList, .Names=unMangleName(basename(fileList)))
}

dbNames<-function(e){
        names(e$objlist)
}

dbSetObjList<-function(e, objlist){
        assign('objlist',as.list(objlist),envir=e)
}

dbInsertNames<-function(e,names,values){
        e$objlist[names]=values
}

dbRemoveNames<-function(e,names){
        e$objlist[names]=NULL
}

dbValForKeys<-function(e, key){
        vals=e$objlist[key]
        nulls=sapply(vals,is.null)
        if(any(nulls)) stop("some keys are missing!")
        else unlist(vals)
}

################################################################################
## Interface functions

setMethod("dbInsert",
          signature(db = "filehashRDS2", key = "character", value = "ANY"),
          function(db, key, value, safe = TRUE, ...) {
                  if(!safe)
                          message("RDS2: Ignoring 'safe = FALSE'")
                  of <- objectFile(db, key)
                  od <- dirname(of)

                  # write to temporary file within database directory
                  # this will be on same filesystem as main database
                  # allowing file.rename to be used
                  writefile <- tempfile(pattern='.RDS2tmp',tmpdir=file.path(db@dir))
                  con <- if(db@compression) gzfile(writefile, "wb") else file(writefile, "wb")

                  writestatus <- tryCatch({
                          serialize(value, con, xdr=db@xdr)
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
                  dbInsertNames(db@objects,key, of)
                  return(rval)
          })

setMethod("dbList", "filehashRDS2",
          function(db, ...) {
                  ## list all keys/files in the database
                  dbNames(db@objects)
          })

setMethod("dbExists", signature(db = "filehashRDS2", key = "character"),
          function(db, key, ...) {
                  key%in%dbNames(db@objects)
          })

setMethod("dbDelete", signature(db = "filehashRDS2", key = "character"),
          function(db, key, ...) {
                  ofile <- objectFile(db, key)
                  
                  ## remove the key from the object list and delete
                  ## the file
                  dbRemoveNames(db@objects, key)
                  status <- file.remove(ofile)
                  invisible(isTRUE(all(status)))
          })

setMethod("length", "filehashRDS2",
          function(x) {
                  length(x@objects$objlist)
          })
