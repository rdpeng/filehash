#include <R.h>
#include <Rinternals.h>
#include <fcntl.h>
#include <unistd.h>

SEXP lock_file(SEXP filename)
{
	int fd;
	SEXP status;

	if(!isString(filename))
		error("'filename' should be character");
	PROTECT(status = allocVector(INTSXP, 1));

	fd = open(CHAR(STRING_ELT(filename, 0)),
		  O_WRONLY | O_CREAT | O_EXCL, 0666);
	INTEGER(status)[0] = fd;
	close(fd);
	UNPROTECT(1);
	return status;
}
