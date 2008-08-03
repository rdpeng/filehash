createQ <- function(filename) {
        dbCreate(filename, "DB1")
        dbInit(filename, "DB1")
}

putQ <- function(qdb, keys) {
        len <- length(keys)
        
        for(i in seq_along(keys)) {
                nextkey <- if(i == len)
                        NULL
                else
                        keys[i + 1]
                obj <- list(key = keys[i],
                            nextkey = nextkey)
                dbInsert(db, obj)
        }
}
