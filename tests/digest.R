## Verify 'digest' output

suppressMessages(library(filehash))
suppressMessages(library(digest))

x <- 1:100
bytes <- serialize(x, NULL)
digest(bytes, algo = "sha1", skip = 14L, serialize = FALSE)

filehash:::sha1(x)

