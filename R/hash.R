sha1 <- function(object, skip = 14L) {
        bytes <- serialize(object, NULL)
        digest(bytes, algo = "sha1", skip = skip, serialize = FALSE)
}


sha1_file <- function(filename, skip = 0L) {
	digest(filename, algo = "sha1", file = TRUE)
}
