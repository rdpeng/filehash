#include <R.h>
#include <Rinternals.h>
#include "sha1.h"

/* 
 * This code is adapted from the 'digest.c' code in the 'digest'
 * package by Dirk Eddelbuettel <edd@debian.org> with contributions by
 * Antoine Lucas, Jarek Tuszynski, Henrik Bengtsson and Simon Urbanek
 */

SEXP sha1_object(SEXP object, SEXP skip_bytes)
{
	char output[41];  /* SHA-1 is 40 bytes + '\0' */
	int i, skip;
	SEXP result;
	sha1_context ctx;
	unsigned char buffer[20];
	Rbyte *data;
	int nChar = length(object);

	PROTECT(object = coerceVector(object, RAWSXP));
	data = RAW(object);
	PROTECT(skip_bytes = coerceVector(skip_bytes, INTSXP));
	skip = INTEGER(skip_bytes)[0];
	
	if(skip > 0) {
		if(skip >= nChar)
			nChar = 0;
		else {
			nChar -= skip;
			data += skip;
		}
	}
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *) data, nChar);
	sha1_finish(&ctx, buffer);

	for(i=0; i < 20; i++)
		sprintf(output + i * 2, "%02x", buffer[i]);

	PROTECT(result = allocVector(STRSXP, 1));
	SET_STRING_ELT(result, 0, mkChar(output));
	UNPROTECT(3);

	return result;
}


SEXP sha1_file(SEXP filename, SEXP skip_bytes)
{
	char output[41];  /* SHA-1 is 40 bytes + '\0' */
	int nChar, i, skip;
	FILE *fp;
	SEXP result;
	sha1_context ctx;
	unsigned char buf[1024];
	unsigned char sha1sum[20];

	PROTECT(skip_bytes = coerceVector(skip_bytes, INTSXP));
	PROTECT(filename = coerceVector(filename, STRSXP));

	skip = INTEGER(skip_bytes)[0];

	if(!(fp = fopen(CHAR(STRING_ELT(filename, 0)), "rb"))) 
		error("unable to open input file");
	if (skip > 0) 
		fseek(fp, skip, SEEK_SET);
	sha1_starts(&ctx);

	while((nChar = fread(buf, 1, sizeof(buf), fp)) > 0)
		sha1_update(&ctx, buf, nChar);

	fclose(fp);
	sha1_finish(&ctx, sha1sum);
	
	for(i=0; i < 20; i++)
		sprintf(output + i * 2, "%02x", sha1sum[i]);

	PROTECT(result = allocVector(STRSXP, 1));
	SET_STRING_ELT(result, 0, mkChar(output));
	UNPROTECT(3);

	return result;
}
