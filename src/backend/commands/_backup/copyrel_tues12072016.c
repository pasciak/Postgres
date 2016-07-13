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
	COPY_RELATION					/* to/from file (or a piped program) */

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

static List *CopyGetAttnums(TupleDesc tupDesc, Relation rel,
			   List *attnamelist);

static CopyState PrepareCopyRel(Relation rel_from, Relation rel_in, Node *raw_query,
		  const char *queryString, const Oid queryRelId, const Oid queryRelId2 ,List *attnamelist, List *options);

static CopyState BeginCopyRel(Relation rel_from, Relation rel_in, Node *query,
		  const char *queryString, const Oid queryRelId, const Oid queryRelId2 ,List *attnamelist, List *options);


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
		
	cstate = PrepareCopyRel(rel_from, rel_in, query, queryString, relid, relid2, stmt->attlist, stmt->options);




//	*processed = ProcessCopyRel(cstate);	/* copy from database to file */
//	EndCopyRel(cstate);

 	if (rel_from != NULL)
		heap_close(rel_from, AccessShareLock);
	if (rel_in != NULL)
		heap_close(rel_in, NoLock);

	elog(LOG, "123");

	return relid;

}


/*

This function prepares the copystate 

*/


static CopyState PrepareCopyRel(Relation rel_from, Relation rel_in, Node *raw_query,
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
	int			num_phys_attrs_in;

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

	/* Process ooptions, do it later!  */

	//ProcessCopyOptions(cstate, is_from, options);
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

// make the OID check here ?

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

		Assert(query->commandType == CMD_SELECT);
		Assert(query->utilityStmt == NULL);


		plan = planner(query, 0, NULL);

/*

We have the querry planned, and now we wanr to convert the COPY relation TO relation
to a query based COPY statement

Note that at the beginning of DoCopy the relations are locked. The planner will
search for the locked relation again, and so we need to make sure that
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
a modified source that consist of only the rows under consideration.


*/
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* 

		Create dest receiver for COPY OUT 

		Need to figure this out: in the original copy.c, COPY query TO
		sends data to file. In here, we send data to another relation.
		Should we change the argument that the CreateDestReveiver gets, so 
		that the

		   case DestIntoRel:
               return CreateIntoRelDestReceiver(NULL);

        case from the dest.c is called?      
   

		*/

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


	cstate->attnumlist = CopyGetAttnums(tupDesc_from, cstate->rel_from, attnamelist);

	num_phys_attrs_from = tupDesc_from->natts;


	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * rel can be NULL ... it's only used for error reports.
 * 
 *	Note that we want this only for the rel_from case, as this the 
 *	relation that . Hence for now this function will be left unmodified, as in copy.c
 *
 */


static List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */

		Form_pg_attribute *attr = tupDesc->attrs;
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (attr[i]->attisdropped)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				if (tupDesc->attrs[i]->attisdropped)
					continue;
				if (namestrcmp(&(tupDesc->attrs[i]->attname), name) == 0)
				{
					attnum = tupDesc->attrs[i]->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
					errmsg("column \"%s\" of relation \"%s\" does not exist",
						   name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}
