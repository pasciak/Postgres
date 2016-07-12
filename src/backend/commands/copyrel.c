#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/copyrel.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_relation.h"
#include "nodes/makefuncs.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"

/*

Do I need this?

*/

typedef enum CopyDest
{
	COPY_FILE,					/* to/from file (or a piped program) */
	COPY_OLD_FE,				/* to/from frontend (2.0 protocol) */
	COPY_NEW_FE					/* to/from frontend (3.0 protocol) */
} CopyDest;

/* 

DestReceiver for COPY (query) TO , because the COPY __ TO  needs to go somewhere

NOTE: lookup dest.c and DestReceiver() function cases , each of them depending
on the tyoe of destination container. We want relation, dont we?

*/

typedef struct
{
    DestReceiver pub;           /* publicly-known function pointers */
    CopyState   cstate;         /* CopyStateData for the command */
    uint64      processed;      /* # of tuples processed */
} DR_copy;
  


// copystate from adopted from copy.c , minor additions here
typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool		fe_eof;			/* true if detected end of copy data */

	/* parameters from the COPY command */
	Relation 	rel;			/* relation to copy from or to */

// these two are used for COPYREL call

	Relation	rel_from;			/* relation to copy from */
	Relation	rel_in;			/* relation to copy to or from */

	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDIN/STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */

	bool		oids;			/* include OIDs? */


	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	int			cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */


} CopyStateData;

// Function prototypes

static CopyState BeginCopyRel(Relation rel_from, Relation rel_in, Node *raw_query,
		  const char *queryString, const Oid queryRelId, const Oid queryRelId2 ,List *attnamelist,
		  List *options);


/*

main function that is called from utility.c and performs
the copy between relations

CopyRelStmt -> see parsenodes.h for definition 

Load contents of one table into other table, and exectute
the arbitrary statement query

*/

Oid DoCopyRel(const CopyRelStmt *stmt, const char *queryString, uint64 *processed){ 

	CopyState	cstate;

// we consider two relations in this case;

	Relation	rel_from;
	Relation 	rel_in;	
	Oid			relid;
	Oid			relid2;

	Node	   *query = NULL;
	List	   *range_table = NIL;

	Assert((stmt->relation_in) && (!stmt->query || !stmt->relation_from));

/* 

the standard procedure for the permission check, only the superuser can use the copy function

*/
	if (!superuser())
			{
			ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				errmsg("must be superuser to use the COPY function"),
				errhint("")));
	}

	if (!stmt->query)

	{

		Assert(stmt->relation_from);

/* open and lock tables, the lock type depending on
whether we read or write to table */

		rel_from = heap_openrv(stmt->relation_from,
						 AccessShareLock);
		relid=RelationGetRelid(rel_from);

		rel_in = heap_openrv(stmt->relation_in,
						  RowExclusiveLock);
		relid2=RelationGetRelid(rel_in);

	}

	else if (stmt->query)
	{

		Assert(!stmt->relation_from);		
		query = stmt->query;
		relid = InvalidOid;
		rel_from = NULL;

		Assert(stmt->relation_in);
	/*
		we are using SELECT statement to load data
		into relation_in table, hence we need to 
		lock this table for concurrent use

	*/	
		rel_in = heap_openrv(stmt->relation_in,
						  RowExclusiveLock);
		relid2=RelationGetRelid(rel_in);
	}
		
	cstate = BeginCopyRel(rel_from, rel_in, query, queryString, relid, relid2, stmt->attlist, stmt->options);

//	*processed = ProcessCopyRel(cstate);	/* copy from database to file */
//	EndCopyRel(cstate);

 	if (rel_from != NULL)
		heap_close(rel_from, AccessShareLock);
	if (rel_in != NULL)
		heap_close(rel_in, NoLock);

	elog(LOG, "123");

	return relid;

}


// we need to open the second/foreign relation somehow


static CopyState BeginCopyRel(Relation rel_from, Relation rel_in, Node *raw_query,
		  const char *queryString, const Oid queryRelId, const Oid queryRelId2 ,List *attnamelist,
		  List *options)
{

/*

All _from are source, all _in are target objects

*/	

	CopyState	cstate;
	TupleDesc	tupDesc_from;
	TupleDesc	tupDesc_in;

	int			num_phys_attrs_from;
	int			num_phys_attrs_from_in;

	MemoryContext oldcontext;

	Query	   *query;


	/* Allocate workspace and zero all fields */
	cstate = (CopyStateData *) palloc0(sizeof(CopyStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPYREL",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

/*

If no SELECT passed, directly get info about both relations under consideration

*/

	if (rel_from!=NULL){

		Assert(!raw_query);

		cstate->rel_from=rel_from;
		cstate->rel_in=rel_in;
		tupDesc_from = RelationGetDescr(cstate->rel_from);
		tupDesc_in = RelationGetDescr(cstate->rel_in);

		query=NULL;
/*
//  keep this OID check ?

		if (cstate->oids && !cstate->rel_from->rd_rel->relhasoids)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("table \"%s\" does not have OIDs",
							RelationGetRelationName(cstate->rel_from))));

		if (cstate->oids && !cstate->reol_in->rd_rel->relhasoids)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("table \"%s\" does not have OIDs",
							RelationGetRelationName(cstate->rel_in))));

*/

	}


	else if (rel_from==NULL){

/*

	set up the querry-modified rel_from and the rel_in 

*/

		List	     *rewritten;
		PlannedStmt  *plan;
		DestReceiver *dest;

		Assert(raw_query);

		cstate->rel_from = NULL;

		if (cstate->oids)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY (SELECT) WITH OIDS is not supported")));


		cstate->rel_in=rel_in;
		tupDesc_in = RelationGetDescr(cstate->rel_in);


		rewritten = pg_analyze_and_rewrite((Node *) copyObject(raw_query),
										   queryString, NULL, 0);

		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result");

		query = (Query *) linitial(rewritten);

		plan = planner(query, 0, NULL);

/*

We have the querry planned, and now we to convert the COPY relation TO relation
to a query based COPY statement

Note that at the beginning of DoCopy the relations are locked. The planner will
search for the locked relation again, and so we need to makes sure that
it now finds the same relation 

*/

		if (queryRelId != InvalidOid)
		{
			
			if (!list_member_oid(plan->relationOids, queryRelId))
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("relation referenced by COPY statement has changed")));
		}


/*

Here we prepare the snapshot. In other words, the SELECT statement derives 
a modified sourcce that consist of only the rows under consideration.


*/
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* Create dest receiver for COPY OUT */

		dest = CreateDestReceiver(DestCopyOut);
		((DR_copy *) dest)->cstate = cstate;

		/* Create a QueryDesc requesting no output */
		cstate->queryDesc = CreateQueryDesc(plan, queryString,
											GetActiveSnapshot(),
											InvalidSnapshot,
											dest, NULL, 0);

		/*
		 * Call ExecutorStart to prepare the plan for execution.
		 *
		 * ExecutorStart computes a result tupdesc for us
		 */
		ExecutorStart(cstate->queryDesc, 0);

/*

Hence we get the query-modfiied set of tuples 

*/

		tupDesc_from = cstate->queryDesc->tupDesc;

	}


	/* Extract options from the statement node tree */
	//ProcessCopyOptions(cstate, is_from, options);


	// need to get query from raw querry and querrystring




	if (rel_from !=NULL && query==NULL){




	} 
	


	else if (query!=NULL){



	}
	
	return cstate;
}

