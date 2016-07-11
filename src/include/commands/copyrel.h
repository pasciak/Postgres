/*-------------------------------------------------------------------------
 *
 * fdcopy.h
 *	  Definitions for using the POSTGRES copy between two relations command.
 *
 * src/include/commands/fdcopy.h
 *
 *-------------------------------------------------------------------------
 */

// this header file includes declarations of necessary functions

#ifndef COPYREL_H
#define COPYREL_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"

/* CopyStateData is private in commands/fdcopy.c */


// define the *CopyStateFd type to be used later in fdcopy.c
typedef struct CopyStateData *CopyStateRel;


//
extern Oid DoCopyRel(const CopyStmt *stmt, const char *queryString,
	   uint64 *processed);

extern void ProcessCopyRelOptions(CopyState cstate, bool is_from, List *options);
extern CopyState BeginCopyRel(Relation rel, const char *filename,
			  bool is_program, List *attnamelist, List *options);
extern void EndCopyRel(CopyState cstate);
extern bool NextCopyRel(CopyState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls, Oid *tupleOid);
extern bool NextCopyRelRawFields(CopyState cstate,
					  char ***fields, int *nfields);
extern void CopyRelErrorCallback(void *arg);

extern DestReceiver *CreateCopyDestReceiver(void);

#endif   /* COPY_BETWEEN */
