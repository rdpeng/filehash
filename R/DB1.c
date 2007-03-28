#define NEED_CONNECTION_PSTREAMS

#include <R.h>
#include <Rinternals.h>

SEXP read_key_map(SEXP filename, SEXP map, SEXP filesize) 
{
	SEXP key, pos, datalen;
	FILE *fp;	
	struct R_inpstream_st in;
	
	if(!isEnvironment(map))
		error("rho should be an environment");

	PROTECT(filesize = coerceVector(filesize, INTSXP));
	PROTECT(pos = allocVector(INTSXP, 1));
	INTEGER(pos)[0] = 0;

	fp = fopen(CHAR(STRING_ELT(filename, 0)), "rb");
	R_InitFileInPStream(&in, fp, R_pstream_any_format, NULL, NULL);

	while(INTEGER(pos)[0] < INTEGER(filesize)[0]) {
		key = R_Unserialize(&in);
		/* Rprintf("Key: %s\n", CHAR(STRING_ELT(key, 0))); */

		datalen = R_Unserialize(&in);
		/* Rprintf("Data len: %d\n", INTEGER(datalen)[0]); */

		INTEGER(pos)[0] = ftell(fp);
		/* Rprintf("Pos: %d\n", INTEGER(pos)[0]); */
	
		defineVar(install(CHAR(STRING_ELT(key, 0))), duplicate(pos), map);

		fseek(fp, INTEGER(datalen)[0], SEEK_CUR);
		INTEGER(pos)[0] = INTEGER(pos)[0] + INTEGER(datalen)[0];
	}
	UNPROTECT(2);
	fclose(fp);
	return map;
}
