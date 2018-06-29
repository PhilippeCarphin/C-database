/** -- btree.h --- */
#include "cdata-ddl-compiler/cdata.h" // NOT IN ACTUAL BOOK
#define MXTREES 20
#define NODE 512
#define ADR sizeof(RPTR)

/************* btree prototypes ***********/
int btree_init(char *);
int btree_close(int);
void build_b(char *, int);
RPTR locate(int, char *);
int deletekey(int, char *, RPTR);
int insertkey(int, char *, RPTR, int);
RPTR nextkey(int);
RPTR prevkey(int);
RPTR firstkey(int);
RPTR lastkey(int);
void keyval(int, char *);
RPTR currkey(int);

/******************** the btree node structure *********************/
typedef struct treenode {
   int nonleaf; /* boolean value to know if this node is a leaf */
   RPTR prntnode; /* Parent node */
   RPTR lfsib; /* left sibling node */
   RPTR rtsib; /* right sibling node */
   int keyct; /* number of keys */
   RPTR key0; /* Node # of kyes < 1st key this node */
   char keyspace[NODE - ((sizeof(int) * 2) + (ADR * 4))];
   char spil[MXKEYLEN]; /* for insertion excess */
} BTREE;

/**************** structure of the btree header node *************/
typedef struct treehdr {
   RPTR rootnode;
   int keylength;
   int m; /* Max keys per node */
   RPTR rlsed_node; /* next released node */
   RPTR endnode; /* next unassigned node */
   int locked; /* Boolean for determining locking */
   RPTR leftmost; /* Leftmost node */
   RPTR rightmost; /* Rightmost node */
} HEADER;
