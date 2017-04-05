#include <stdlib.h> // for NULL
#include <R_ext/Rdynload.h>
#include <Rinternals.h>

SEXP sha1_object(SEXP object, SEXP skip_bytes);
SEXP sha1_file(SEXP filename, SEXP skip_bytes);
SEXP read_key_map(SEXP filename, SEXP map, SEXP filesize, SEXP pos);
SEXP lock_file(SEXP filename);

static const R_CallMethodDef CallEntries[] = {
	{"lock_file", (DL_FUNC) &lock_file, 1},
	{"read_key_map", (DL_FUNC) &read_key_map, 4},
	{"sha1_object", (DL_FUNC) &sha1_object, 2},
	{"sha1_file", (DL_FUNC) &sha1_file, 2},
	{NULL, NULL, 0}
};

void R_init_filehash(DllInfo *info)
{
	R_registerRoutines(info, NULL, CallEntries, NULL, NULL);
	R_useDynamicSymbols(info, FALSE);
	R_forceSymbols(info, TRUE);
	
}
