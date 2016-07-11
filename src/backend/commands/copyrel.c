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


typedef enum CopyDest
{
	COPY_FILE,					/* to/from file (or a piped program) */
	COPY_OLD_FE,				/* to/from frontend (2.0 protocol) */
	COPY_NEW_FE					/* to/from frontend (3.0 protocol) */
} CopyDest;

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL
} EolType;

// copystate from adopted from copy.c , minor additions here
typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool		fe_eof;			/* true if detected end of copy data */
	EolType		eol_type;		/* EOL type of input */

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

	/*
	 * Working state for COPY FROM
	 */
	AttrNumber	num_defaults;
	bool		file_has_oids;
	FmgrInfo	oid_in_function;
	Oid			oid_typioparam;
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	int		   *defmap;			/* array of default att numbers */
	ExprState **defexprs;		/* array of default att expressions */
	bool		volatile_defexprs;		/* is any of defexprs volatile? */
	List	   *range_table;

	/*
	 * These variables are used to reduce overhead in textual COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int			max_fields;
	char	  **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.
	 */
	StringInfoData line_buf;
	bool		line_buf_converted;		/* converted to server encoding? */
	bool		line_buf_valid; /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).  CopyReadLine parses this data sufficiently to
	 * locate line boundaries, then transfers the data to line_buf and
	 * converts it.  Note: we guarantee that there is a \0 at
	 * raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */
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

	//elog(LOG, "dupaduapduapduapudapudap");

	return relid;

}


// we need to open the second/foreign relation somehow


static CopyState BeginCopyRel(Relation rel_from, Relation rel_in, Node *raw_query,
		  const char *queryString, const Oid queryRelId, const Oid queryRelId2 ,List *attnamelist,
		  List *options)
{

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

If no SELECT passed, directlu get info about both relations under consideration

*/

	if (rel_from!=NULL){

		Assert(!raw_query);

		cstate->rel_from=rel_from;
		cstate->rel_in=rel_in;
		tupDesc_from = RelationGetDescr(cstate->rel_from);
		tupDesc_in = RelationGetDescr(cstate->rel_in);

		query=NULL;
/*
// shall we keep this check

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

	set up the querry and the rel_in relation

*/

		List	   *rewritten;
		PlannedStmt *plan;
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
