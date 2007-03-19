######################################################################
## Class 'filehashDB'

createDB <- function(dbName) {
    mapfile <- generateMapFile(dbName)
    datafile <- generateDataFile(dbName)
    
    ## Database doesn't already exist
    if(!any(file.exists(c(mapfile, datafile)))) {
        map <- list()
        con <- gzfile(mapfile, "wb")
        tryCatch({
            serialize(map, con)
        }, finally = close(con))
        
        createEmptyFile(datafile)
    }
    else
        message("database ", sQuote(dbName), " already exists")
    TRUE
}

initializeDB <- function(dbName) {
    mapfile <- generateMapFile(dbName)
    datafile <- generateDataFile(dbName)

    
    obj <- new("filehashDB", datafile = datafile,
               mapfile = mapfile, name = basename(dbName))
    if(!isTRUE(validObject(obj))) {
        msg <- strwrap(paste("database not valid; if your 'DB'",
                             "database was created before version",
                             "0.6-2 of 'filehash' you need to use",
                             "'convertDB' to convert your database",
                             "to the new format"))
        message(msg)
    }
    obj
}

setClass("filehashDB",
         representation(datafile = "character",
                        mapfile = "character"),
         contains = "filehash"
         )

setValidity("filehashDB",
            function(object) {
                if(!file.exists(object@datafile))
                    return(paste("datafile", object@datafile,
                                 "does not exist"))
                if(!file.exists(object@mapfile))
                    return(paste("mapfile", object@mapfile, "does not exist"))
                TRUE
            })

generateMapFile <- function(dbName) {
    stopifnot(length(dbName) == 1)
    paste(dbName, "idx", sep = "")

}

generateDataFile <- function(dbName) {
    stopifnot(length(dbName) == 1)
    paste(dbName, "rdb", sep = "")
}


readData <- function(con, start, len) {
    if(!isOpen(con)) 
        stop("connection is not open")
    if(!isSeekable(con))
        stop("cannot seek on connection")
    seek(con, start)
    data <- readBin(con, "raw", len)
    unserialize(data)  ## Works in R >= 2.3.0
}


appendData <- function(con, data) {
    if(!isOpen(con)) 
        stop("connection is not open")
    if(!isSeekable(con))
        stop("cannot seek on connection")
    start <- getEndPos(con)

    byteData <- toBytes(data)
    writeBin(byteData, con)

    c(start, length(byteData))
}

getEndPos <- function(con) {
    seek(con, 0, "end")
    seek(con)
}

convertDB <- function(name) {
    oldMapfile <- paste(name, "idx", sep = ".")
    oldDatafile <- paste(name, "rdb", sep = ".")
    if(!file.exists(oldMapfile))
        stop("map file ", sQuote(oldMapfile), " does not exist")
    if(!file.exists(oldDatafile))
        stop("data file ", sQuote(oldDatafile), " does not exist")
    newDatafile <- generateDataFile(name)
    newMapfile <- generateMapFile(name)
    
    if(!file.rename(oldDatafile, newDatafile))
        stop("unable to rename data file")
    if(!file.rename(oldMapfile, newMapfile))
        stop("unable to rename map file")
    if(!convertDBIndex(newMapfile))
        stop("unable to convert index file")
    TRUE
}

convertDBIndex <- function(name) {
    con <- gzfile(name, "rb")
    tryCatch({
        env <- unserialize(con)
    }, finally = close(con))

    ## Convert environment to list
    map <- mget(ls(env, all.names = TRUE), env)
    tfile <- tempfile()

    con <- gzfile(tfile, "wb")
    tryCatch({
        serialize(map, con)
    }, finally = close(con))

    file.rename(tfile, name)
}

setGeneric("getDataCon", function(db, ...) standardGeneric("getDataCon"))

setMethod("getMap", "filehashDB", function(db) {
    con <- gzfile(db@mapfile, "rb")
    tryCatch({
        unserialize(con)
    }, finally = close(con))
})
setMethod("getDataCon", "filehashDB", function(db) file(db@datafile))


######################################################################
## Interface functions

setMethod("dbInsert",
          signature(db = "filehashDB", key = "character", value = "ANY"),
          function(db, key, value) {
              map <- getMap(db)

              ## Write data chunk and update map
              con <- getDataCon(db)
              open(con, "ab")
              
              tryCatch({
                  map[[key]] <- appendData(con, value)
              }, finally = close(con))
                  
              ## Write index file
              con <- gzfile(db@mapfile, "wb")
              tryCatch({
                  serialize(map, con)
              }, finally = close(con))
              TRUE
          }
          )

setMethod("dbFetch", signature(db = "filehashDB", key = "character"),
          function(db, key) {
              if(!dbExists(db, key))
                  stop(sQuote(key), " not in database")

              map <- getMap(db)
              idx <- map[[key]]
              con <- getDataCon(db)
              open(con, "rb")

              tryCatch({
                  readData(con, idx[1], idx[2])
              }, finally = close(con))
          })

setMethod("dbExists", signature(db = "filehashDB", key = "character"),
          function(db, key) {
              map <- getMap(db)
              key %in% names(map)
          }
          )

setMethod("dbList", "filehashDB",
          function(db) {
              map <- getMap(db)
              names(map)
          }
          )

setMethod("dbDelete", signature(db = "filehashDB", key = "character"),
          function(db, key) {
              map <- getMap(db)
              map[[key]] <- NULL

              con <- gzfile(db@mapfile, "wb")
              tryCatch({
                  serialize(map, con)
              }, finally = close(con))
              TRUE
          }
          )

setMethod("dbReorganize", "filehashDB",
          function(db) {
              rval <- TRUE
              tempdir <- tempfile()
              dir.create(tempdir)
              tempdbName <- file.path(tempdir, dbName(db))
              
              if(!dbCreate(tempdbName, type = "DB"))
                  return(FALSE)
              tempdb <- dbInit(tempdbName, type = "DB")
              
              for(key in dbList(db)) 
                  dbInsert(tempdb, key, dbFetch(db, key))

              status <- file.copy(tempdb@mapfile, db@mapfile, overwrite = TRUE)
              
              if(!isTRUE(all(status))) {
                  warning("problem copying map file")
                  rval <- FALSE
              }
              status <- file.copy(tempdb@datafile, db@datafile, overwrite = TRUE)
              if(!isTRUE(all(status))) {
                  warning("problem copying data file")
                  rval <- FALSE
              }
              rval
          }
          )

setMethod("dbUnlink", "filehashDB",
          function(db) {
              mapfile <- generateMapFile(dbName(db))
              datafile <- generateDataFile(dbName(db))

              tryCatch({
                  unlink(mapfile)
                  unlink(datafile)
                  TRUE
              }, error = function(cond) {
                  cat(as.character(cond))
                  FALSE
              })
          })
