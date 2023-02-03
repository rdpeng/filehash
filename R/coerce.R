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

toDBType <- function(from, type, dbpath = NULL) {
    if(is.null(dbpath))
        dbpath <- dbName(from)
    if(!dbCreate(dbpath, type = type))
        stop("could not create ", type, " database")
    db <- dbInit(dbpath, type = type)
    keys <- dbList(from)
    
    for(key in keys)
        dbInsert(db, key, dbFetch(from, key))
    invisible(db)
}

#' Coerce a filehash database
#' 
#' Coerce a filehashDB1 database to filehashRDS format
#' 
#' @name coerceDB1RDS
#' @param from a filehashDB1 database object
#' @exportMethod coerce
#' @aliases coerce,filehashDB1,filehashRDS-method
setAs("filehashDB1", "filehashRDS",
      function(from) {
          dbpath <- paste(dbName(from), "RDS", sep = "")
          toDBType(from, "RDS", dbpath)
      })
      
#' Coerce a filehash database
#' 
#' Coerce a filehashDB1 database to a list object
#' 
#' @name coerceDB1list
#' @param from a filehashDB1 database object
#' @exportMethod coerce
#' @aliases coerce,filehashDB1,list-method
setAs("filehashDB1", "list",
      function(from) {
          keys <- dbList(from)
          dbMultiFetch(from, keys)
      })

#' Coerce a filehash database
#' 
#' Coerce a filehash database to a list object
#' 
#' @name coercelist
#' @param from a filehash database object
#' @exportMethod coerce
#' @aliases coerce,filehash,list-method
setAs("filehash", "list",
      function(from) {
              env <- new.env(hash = TRUE)
              dbLoad(from, env)
              as.list(env, all.names = TRUE)
      })



