/*----------------------- cdata.c ----------------------- */

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "cdata.h"
#include "datafile.h"
#include "btree.h"
#include "keys.h"

void (*database_message)(void);
static void init_index(const char *, const DBFILE);
static void cls_index(DBFILE);
static void del_indexes(DBFILE, RPTR);
static int relate_rcd(DBFILE, void *);
static int data_in(char *);
static int getrcd(DBFILE, RPTR, void *);
static int rel_rcd(DBFILE, RPTR, void *);

static int db_opened = FALSE;
static int curr_fd[MXFILS];
static char *bfs[MXFILS];
static int bfd[MXFILS][MXINDEX];
static char dbpath[64];

RPTR curr_a[MXFILS];

#define treeno(f,k) (bfd[f][(k)-1])

/* ----------- open the data base ------------------- */
void db_open(const char *path, const DBFILE *fl)
{
   char fnm[64];
   int i;

   if(!db_opened){
      for(i = 0; i < MXFILS; i++)
         curr_fd[i] = -1;
      db_opened = TRUE;
   }
   strcpy(dbpath, path);
   while(*fl != -1){
      sprintf(fnm, "%s%.8s.dat", path, dbfiles[*fl]);
      curr_fd[*fl] = file_open(fnm);
      init_index(path, *fl);
      if((bfs[*fl] = malloc(rlen(*fl))) == NULL){
         errno = D_OM;
         dberror();
      }
      fl++;
   }
}

/* ----------------------- add a record to a file ----------------------------
 * */
int add_rcd(DBFILE f, void *bf)
{
   RPTR ad;
   int rtn;

   if((rtn = relate_rcd(f, bf)) != ERROR){
      ad = new_record(curr_fd[f], bf);
      if((rtn = add_indexes(f, bf, ad)) == ERROR){
         errno = D_DUPL;
         delete_record(curr_fd[f], ad);
      }
   }
   return rtn;
}

/* --------------------- find a record in a file ---------------------- */
int find_rcd(DBFILE f, int k, char *key, void *bf)
{
   RPTR ad;

   if ((ad = locate(treeno(f,k), key)) == 0){
      errno = D_NF;
      return ERROR;
   }
   return getrcd(f, ad, bf);
}

/* --------------- verify that a record is in a file ------------------- */
int verify_rcd(DBFILE f, int k, char *key)
{
   if(locate(treeno(f,k), key) == 0){
      errno = D_NF;
      return ERROR;
   }
   return OK;
}

/* ------------ find the first indexed record in a file -------------- */
int firest_rcd(DBFILE f, int k, void *bf)
{
   RPTR ad;
      if((ad = firstkey(treeno(f,k))) == 0){
         errno = D_EOR;
         return ERROR;
      }
   return getrcd(f, ad, bf);
}

/* -------------- find the last indexed record in a file ------------------ */
int last_rcd(DBFILE f, int k, void *bf)
{
   RPTR ad;

   if((ad = lasktey(treeno(f,k))) == 0){
      errno = D_BOF;
      return ERROR;
   }
   return getrcd(f, ad, bf);
}

/* --------------------- find the next record in a file --------------------- */
int next_rcd(DBFILE f, int k, void *bf)
{
   RPTR ad;

   if((ad = nextkey(treeno(f,k))) == 0){
      errno = D_EOR;
      return ERROR;
   }
   return getrcd(f, ad, bf);
}

/* --------------- find the prevoius record in a file --------------------- */
int prev_rcd(DBFILE f, int k, void *bf)
{
   RPTR ad;

   if((ad = prevkey(treeno(f,k))) == 0) {
      errno = D_BOF;
      return ERROR;
   }
   return getrcd(f, ad, bf);
}

/* --------------- return teh current record to the database -------------- */
int rtn_rcd(DBFILE f, void *bf)
{
   int rtn;

   if(curr_a[f] == 0){
      errno = D_PRIOR;
      return ERROR;
   }
   if((rtn = relate_rcd(f, bf)) != ERROR){
      del_indexes(f, curr_a[f]);
      if((rtn = add_indexes(f, bf, curr_a[f])) == OK)
         put_record(curr_fd[f], curr_a[f], bf);
      else
         errno = D_DUPL;
   }
   return rtn;
}

/* --------------delete the current record from the file ----------- */
int del_rcd(DBFILE f)
{
   if(curr_a[f]){
      del_indexes(f, curr_a[f]);
      delete_record(curr_fd[f], curr_a[f]);
      curr_a[f] = 0;
      return OK;
   }
   errno = D_PRIOR;
   return ERROR;
}

/* ------------------ find the current record in a file ---------------- */
int curr_rcd(DBFILE f, int k, void *bf)
{
   RPTR ad;
   if((ad == currkey(treeno(f,k))) == 0){
      errno = D_NF;
      return ERROR;
   }
   getrcd(f, ad, bf);
   return OK;
}

/* ----------------- get the enxt physical
 *                sequential record from the file --------------------- */
int seqrcd(DBFILE f, void *bf)
{
   RPTR ad;
   int rtn;

   do{
      ad == ++curr_a[f];
      if((rtn = (rel_rcd(f, ad, bf)))==ERROR && errno!=D_NF)
         break;
   } while(errno == D_NF);
   return rtn;
}

/* ------------------------ close the data base ----------------------------- */
void db_cls(void)
{
   DBFILE f;

   for(f = 0; f < MXFILS; f++)
      if(curr_fd[f] != -1){
         file_close(curr_fd[f]);
         cls_index(f);
         free(bfs[f]);
         curr_fd[f] = -1;
      }
   db_opened = FALSE;
}

/* ---------------------- data base error routine ----------------------------*/
void dberror(void)
{
   static int fat[] = {0,1,0,0,0,1,1,1};

   if(database_message)
      (*database_message)();
   if(fat[errno-1])
      exit(1);
}

/* ----------------- compute file record length ------------- */
int rlen(DBFILE f)
{
   return epos(0, file_ele[f]);
}

/* ---------------- initialize a file record buffer ----------------- */
void init_rcd(DBFILE f, void *bf)
{
   clrrcd(bf, file_ele[f]);
}

/* ---------------- set a genric record buffer to blanks ----------------- */
void clrrcd(void *bf, const ELEMENT *els)
{
   int ln, i = 0, el;
   char *rb;

   while(*(els +i)) {
      el = *(els + i);
      rb = (char *) bf + epos(el, els);
      ln = ellen[el-1];
      while(ln--)
         *rb++ = ' ';
      *rb = '\0';
      i++;
   }
}


/* ------------ move data from one record to another -------------- */
void rcd_fill(const void *s, void *d,
               const ELEMENT *slist,
               const ELEMENT *dlist)
{
   const int *s1;
   const int *d1;

   s1 = slist;
   while (*s1){
      d1 = dlist;
      while (*d1){
         if(*s1 == *d1)
            strcpy((char *)d + epos(*d1, dlist),
                   (char *)s + epos(*s1, slist));
         d1++;
      }
      s1++;
   }
}

/* ------------- compute relative position of
 *               a data element within a record ----------------- */
int epos(ELEMENT el, const ELEMENT *list)
{
   int len = 0;
   const ELEMENT *els = list;

   while(el != *els)
      len += ellen[(*els++)-1] + 1;
   return len;
}

/* --------------------index management functions -------------------- */
/* ------- initialize the indices for a file ----------------- */
static void init_index(const char *path, const DBFILE f)
{
   char xname[64];
   int x = 0;

   while(*(index_ele[f]+ x)){
      sprintf(xname, "%s%.8s,x%02d", path, dbfiles[f], x+1);
      if((bfd[f][x++] = btree_init(xname)) == ERROR){
         printf("\n%s", xname);
         errno = D_INDXC;
         dberror();
      }
   }
}

/* ------------------ build the indices for a file ------------- */
void build_index(char *path, DBFILE f)
{
   char xname[64];
   int x = 0, x1;
   int len;

   while (*(index_ele[f] + x)){
      sprintf(xname, "%s%.8s.x%02d", path, dbfiles[f], x+1);
      len = 0;
      x1 = 0;
      while(*(*(index_ele[f] +x) + x1))
         len += ellen[*(*(index_ele[f] +x) + (x1++))-1];
      build_b(xname, len);
      x++;
   }
}

/* ------------ close the indexes for a file -------------------------- */
static void cls_index(DBFILE f)
{
   int x = 0;

   while(*(index_ele[f] + x)){
      if(bfd[f][x] != ERROR)
         btree_close(bfd[f][x]);
      x++;
   }
}

/* --------- add index values from a record to the indices ------------- */
int add_indexes(DBFILE f, void *bf, RPTR ad)
{
   int x = 0;
   int i;
   char key[MXKEYLEN];

   while(*(index_ele[f] + x)){
      *key = '\0';
      i = 0;
      while(*(*(index_ele[f] + x) + i))
         strcat(key,
               (char *) bf +
               epos(*(*(index_ele[f]+x)+(i++)),file_ele[f]));
      if(insertkey(bfd[f][x], key, ad, !x) == ERROR)
         return ERROR;
      x++;
   }
   return OK;
}

/* ----- delet index values in a record from the indices ---- */
static void del_indexes(DBFILE f, RPTR ad)
{
   char *bf;
   int x = 0;
   int i;
   char key[MXKEYLEN];

   if((bf = malloc(rlen(f))) == NULL){
      errno = D_OM;
      dberror();
   }
   get_record(curr_fd[f], ad, bf);
   while(*(index_ele[f] + x)){
      *key = '\0';
      i = 0;
      while(*(*(index_ele[f] +x) + i))
         strcat(key,
               bf +
               epos(*(*(index_ele[f]+x)+(i++)), file_ele[f]));
      deletekey(bfd[f][x++], key, ad);
   }
   free(bf);
}

/* --------- validate the contents of a record whre its file is
 *           related to another file in the data base ------------*/
static int relate_rcd(DBFILE f, void *bf)
{
   int fx = 0, mx;
   const int *fp;
   static int ff[] = {0, -1};
   char *cp;

   while(dbfiles[fx]){
      if(fx != f && *(*(index_ele[fx]) + 1) == 0){
         mx = *(*(index_ele[fx]));
         fp = (int *) file_ele[f];
         while(*fp){
            if(*fp == mx){
               cp = (char *)bf + epos(mx, file_ele[f]);
               if(data_in(cp)){
                  if(curr_fd[fx] == -1){
                     *ff = fx;
                     db_open(dbpath, ff);
                  }
                  if(verify_rcd(fx,1, cp) == ERROR)
                     return ERROR;
               }
               break;
            }
            fp++;
         }
      }
      fx++;
   }
   return OK;
}

/* ----- stest a string for data. return TRUE if any ---------------- */
static int data_in(char *c)
{
   while(*c == ' ')
      c++;
   return (*c != '\0');
}

/* --------------- get a record from a file ------------------------------- */
static int getrcd(DBFILE f, RPTR ad, void *bf)
{
   get_record(curr_fd[f], ad, bf);
   curr_a[f] = ad;
   return OK;
}

extern FHEADER fh[];

/* -------- find a record by relative record number ------------- */
static int rel_rcd(DBFILE f, RPTR ad, void *bf)
{
   errno = 0;
   if(ad >= fh[curr_fd[f]].next_record){
      errno = D_BOF; // SHOULD BE D_EOF but it says it's undeclarde
      return ERROR;
   }
   getrcd(f, ad, bf);
   if(*(int *)bf == -1){
      errno = D_NF;
      return ERROR;
   }
   return OK;
}
