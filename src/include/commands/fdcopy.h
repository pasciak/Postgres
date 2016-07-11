/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy bewtween command.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FDCOPY_H
#define FDCOPY_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"

/* CopyStateData is private in commands/copy.c */
typedef struct CopyStateData *CopyStateFd;

extern Oid DoCopyFd(const CopyStmt *stmt, const char *queryString,
	   uint64 *processed);

extern void ProcessCopyFdOptions(CopyState cstate, bool is_from, List *options);
extern CopyState BeginCopyFd(Relation rel, const char *filename,
			  bool is_program, List *attnamelist, List *options);
extern void EndCopyFd(CopyState cstate);
extern bool NextCopyFd(CopyState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls, Oid *tupleOid);
extern bool NextCopyFdRawFields(CopyState cstate,
					  char ***fields, int *nfields);
extern void CopyFdErrorCallback(void *arg);

extern DestReceiver *CreateCopyDestReceiver(void);

#endif   /* COPY_BETWEEN */
