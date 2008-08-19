#define NEED_CONNECTION_PSTREAMS

#include <R.h>
#include <Rinternals.h>

SEXP read_key_map(SEXP filename, SEXP map, SEXP filesize, SEXP pos) 
{
	SEXP key, datalen;
	FILE *fp;	
	int status, len;
	struct R_inpstream_st in;
	
	if(!isEnvironment(map))
		error("'map' should be an environment");
	if(!isString(filename))
		error("'filename' should be character");

	PROTECT(filesize = coerceVector(filesize, INTSXP));
	PROTECT(pos = coerceVector(pos, INTSXP));

	fp = fopen(CHAR(STRING_ELT(filename, 0)), "rb");

	if(INTEGER(pos)[0] > 0) {
		status = fseek(fp, INTEGER(pos)[0], SEEK_SET);

		if(status < 0)
			error("problem with initial file pointer seek");
	}
	
	/* Initialize the incoming R file stream */
	R_InitFileInPStream(&in, fp, R_pstream_any_format, NULL, NULL);

	while(INTEGER(pos)[0] < INTEGER(filesize)[0]) {
		PROTECT(key = R_Unserialize(&in));
		PROTECT(datalen = R_Unserialize(&in));
		len = INTEGER(datalen)[0];

		/* calculate the position of file pointer */
		INTEGER(pos)[0] = ftell(fp);

		if(len <= 0) {
			/* key has been deleted; set pos to NULL */
			defineVar(install(CHAR(STRING_ELT(key, 0))),
				  R_NilValue, map);
			UNPROTECT(2);
			continue;
		}
		/* create a new entry in the key map */
		defineVar(install(CHAR(STRING_ELT(key, 0))), duplicate(pos), map);

		/* advance to the next key */
		status = fseek(fp, len, SEEK_CUR);

		if(status < 0) {
			fclose(fp);
			error("problem with seek");
		}
		INTEGER(pos)[0] = INTEGER(pos)[0] + len;

		UNPROTECT(2);
	}
	UNPROTECT(2);
	fclose(fp);
	return map;
}
