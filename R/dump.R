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

#' Dump Environment
#' 
#' Dump an enviroment to a filehash database
#' 
#' @param env an environment
#' @param dbName character, name of the filehash database
#' @param list, character vector of object names to be dumped
#' @param data a data frame
#' 
#' @details The \code{dumpEnv} function takes an environment and stores each element of the environment in a \code{filehash} database. Objects dumped to a database can later be loaded via \code{dbLoad} or can be accessed with \code{dbFetch}, \code{dbList}, etc. Alternatively, the \code{with} method can be used to evaluate code in the context of a database.  If a database with name \code{dbName} already exists, objects will be inserted into the existing database (and values for already-existing keys will be overwritten).
#' 
#' @details \code{dumpDF} is different in that each variable in the data frame is stored as a separate object in the database.  So each variable can be read from the database separately rather than having to load the entire data frame into memory.  \code{dumpList} works in a simlar way.
#' 
#' @return An object of class \code{"filehash"} is returned and a database is created.
#' 
#' @aliases dumpImage dumpObjects dumpDF dumpList
#' @name dumpEnv
#'
#' @export
dumpEnv <- function(env, dbName) {
        keys <- ls(env, all.names = TRUE)
        dumpObjects(list = keys, dbName = dbName, envir = env)
}

#' @export
#' @describeIn dumpEnv Dump the Global Environment (analogous to \code{save.image})
#' @param type type of filehash database to create
dumpImage <- function(dbName = "Rworkspace", type = NULL) {
        dumpObjects(list = ls(envir = globalenv(), all.names = TRUE),
                    dbName = dbName, type = type, envir = globalenv())
}

#' @export
#' @describeIn dumpEnv Dump named objects to a filehash database (analogous to \code{save})
#' @param ... R objects to be dumped to a filehash database
#' @param envir environment from which objects are dumped
dumpObjects <- function(..., list = character(0), dbName, type = NULL,
                        envir = parent.frame()) {
        names <- as.character(substitute(list(...)))[-1]
        list <- c(list, names)
        if(!dbCreate(dbName, type))
                stop("could not create database file")
        db <- dbInit(dbName, type)

        for(i in seq(along = list)) 
                dbInsert(db, list[i], get(list[i], envir))
        db
}

#' @export
#' @describeIn dumpEnv Dump data frame columns to a filehash database
dumpDF <- function(data, dbName = NULL, type = NULL) {
        if(is.null(dbName))
                dbName <- as.character(substitute(data))
        dumpList(as.list(data), dbName = dbName, type = type)
}

#' @export
#' @describeIn dumpEnv Dump elements of a list to a filehash database
dumpList <- function(data, dbName = NULL, type = NULL) {
        if(!is.list(data))
                stop("'data' must be a list")
        vnames <- names(data)
        
        if(is.null(vnames) || isTRUE("" %in% vnames))
                stop("list must have non-empty names")
        if(is.null(dbName))
                dbName <- as.character(substitute(data))
        
        if(!dbCreate(dbName, type))
                stop("could not create database file")
        db <- dbInit(dbName, type)

        for(i in seq(along = vnames))
                dbInsert(db, vnames[i], data[[vnames[i]]])
        db
}

