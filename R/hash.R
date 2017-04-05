sha1 <- function(object, skip = 14L) {
	## Setting 'skip = 14' gives us the same results as
	## 'digest(object, "sha1")'
	bytes <- serialize(object, NULL)
	.Call(C_sha1_object, bytes, skip)
}

sha1_file <- function(filename, skip = 0L) {
	.Call(C_sha1_file, filename, skip)
}
