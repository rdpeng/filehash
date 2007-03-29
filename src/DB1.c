#define NEED_CONNECTION_PSTREAMS

#include <R.h>
#include <Rinternals.h>

SEXP read_key_map(SEXP filename, SEXP map, SEXP filesize, SEXP pos) 
{
	SEXP key, datalen;
	FILE *fp;	
	struct R_inpstream_st in;
	
	if(!isEnvironment(map))
		error("rho should be an environment");

	PROTECT(filesize = coerceVector(filesize, INTSXP));
	PROTECT(pos = coerceVector(pos, INTSXP));
	PROTECT(filename = coerceVector(filename, STRSXP));

	fp = fopen(CHAR(STRING_ELT(filename, 0)), "rb");

	if(INTEGER(pos)[0] > 0)
		fseek(fp, INTEGER(pos)[0], SEEK_SET);
	
	/* Initialize the incoming R file stream */
	R_InitFileInPStream(&in, fp, R_pstream_any_format, NULL, NULL);

	while(INTEGER(pos)[0] < INTEGER(filesize)[0]) {
		key = R_Unserialize(&in);
		datalen = R_Unserialize(&in);
		
		/* calculate the position of file pointer */
		INTEGER(pos)[0] = ftell(fp);
	
		/* create a new entry in the key map */
		defineVar(install(CHAR(STRING_ELT(key, 0))), duplicate(pos), map);
		
		/* advance to the next key */
		fseek(fp, INTEGER(datalen)[0], SEEK_CUR);
		INTEGER(pos)[0] = INTEGER(pos)[0] + INTEGER(datalen)[0];
	}
	UNPROTECT(3);
	fclose(fp);
	return map;
}
