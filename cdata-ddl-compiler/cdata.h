/* --------------- cdata.h ---- listing 6.10 p82 ---------------------------- */

#include <stdio.h>

#ifndef CDATA_H
#define CDATA_H

#define MXFILS    11 // maximum number of files in a data base
#define MXELE     100 // Maximum data elements in a file
#define MXINDEX   5 // Maximum indexes per file
#define MXKEYLEN  80 // Maximum key length for indexes
#define MXCAT     3 // Maximum elements per index
#define NAMELEN   31 // datalement name length

/* initialize this to call your function for data base errors */
extern void (*database_message)(void);

#define ERROR -1
#define OK 0

#ifndef TRUE
#define TRUE 1
#define FALSE 0
#endif

typedef long RPTR; // B-tree node and file address

#ifndef APPLICATION_H
typedef int DBFILE;
typedef int ELEMENT;
#endif

/* -- schema as built for the application -- */
extern const char *dbfiles[];       // filenames
extern const char *denames[];       // data element names
extern const char *elmask[];        // Data element masks
extern const char eltype;           // Data element types
extern const int  ellen[];          // Data element lenghts
extern const ELEMENT *file_ele[];   // File data elements
extern const ELEMENT **index_ele[]; // index data elements

/* -- Data base prototypes -- */

/* -- Cdata API functions -- */
void db_open(const char * const DBFILE *);
int add_rcd(DBFILE, void *);
int find_rcd(DBFILE, int, char *, void *);
int veryfy_rcd(DBFILE, int char *);
int first_rcd(DBFILE, int void *);
int last_rcd(DBFILE, int, void *);
int next_rcd(DBFILE, int, void *);
int prev_rcd(DBFILE, int, void *);
int rtn_rcd(DBFILE, void *);
int del_rcd(DBFILE);
int curr_rcd(DBFILE, int, void *);
int seqrcd(DBFILE, void *);
void db_cls(void);
void dberror(void);
int rlen(DBFILE);
void init_rcd(DBFILE, void *);
void clrrcd(void * const ELEMENT *);
int epos(ELEMENT, const ELEMENT *);
void rcd_fill(const void *, void *, const ELEMENT *,
                                           const ELEMENT *);

/* -- functions used by Cdata utility programs -- */
void build_index(char *, DBFILE);
int add_indexes(DBFILE, void *, RPTR);
DBFILE filename(char *);
void name_cvt(char *, char *);
int ellist(int, char **, ELEMENT *);
void clist(FILE *, const ELEMENT *, const ELEMENT *,
                                 void *, const char *);
void test_eop(FILE *, const char *, const ELEMENT *);
void dblist(FILE *, DBFILE, int, const ELEMENT *);

/* -- dbms error codes for errno return -- */
enum dberrors {
   D_NF=1,     // record not found
   D_PRIOR,    // no prior record for this request
   D_EOR,      // end of file
   D_BOF,      // beginning of file
   D_DUPL,     // primary key already existes
   D_OM,       // out of memory
   D_INDXC,    // index corrupted
   D_IOERR     // i/o error
};

#endif


