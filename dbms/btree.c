/***************** btree.c **********************************/
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include "cdata.h"
#include "btree.h"

#define KLEN bheader[trx].keylength
#define ENTLN (KLEN + ADR)

HEADER bheader[MXTREES];
BTREE trnode;

static FILE *fp[MXTREES];
static RPTR currnode[MXTREES];
static int currkno[MXTREES];
static int trx;

/******** local prototypes ********************/
static int btreescan(RPTR *, char *, char **);
static int nodescan(char *, char **);
static int compare_keys(char *, char *);
static RPTR fileaddr(RPTR, char *);
static RPTR leaflevel(RPTR *, char **, int *);
static void implode(BTREE *, BTREE *);
static void redist(BTREE *, BTREE *);
static void adopt(void *, int, RPTR);
static RPTR nextnode(void);
static RPTR scannext(RPTR *, char **);
static RPTR scanprev(RPTR *, char **);
static char *childptr(RPTR, RPTR, BTREE *);
static void read_node(RPTR, void*);
static void write_node(RPTR, void*);
static void bseek(RPTR);
static void memerr(void);

/*************** initiate btree processing *****************/
int btree_init(char *ndx_name)
{
   for(trx = 0; trx < MXTREES; trx++)
      if(fp[trx] == NULL)
         break;
   if(trx == MXTREES)
      return ERROR;
   if((fp[trx] = fopen(ndx_name, "rb+")) == NULL)
      return ERROR;
   fread(&bheader[trx], sizeof(HEADER), 1, fp[trx]);
   /* --- if this tree is locked, something is amiss --- */
   if(bheader[trx].locked){
      fclose(fp[trx]);
      fp[trx] = NULL;
      return ERROR;
   }
   /* ---- lock the btree ----- */
   bheader[trx].locked = TRUE;
   fseek(fp[trx], 0L, SEEK_SET);
   fwrite(&bheader[trx], sizeof(HEADER), 1, fp[trx]);
   currnode[trx] = 0;
   currkno[trx] = 0;
   return trx;
}

/* terminate btree processing */
int btree_close(int tree)
{
   if(tree >= MXTREES || fp[tree] == NULL)
      return ERROR;
   bheader[tree].locked = FALSE;
   fseek(fp[tree], 0L, SEEK_SET);
   fwrite(&bheader[tree], sizeof(HEADER), 1, fp[tree]);
   fclose(fp[tree]);
   fp[tree] = NULL;
   return OK;
}

/* build a new-btree */
void build_b(char *name, int len)
{
   HEADER *bhdp;
   FILE *fp;

   if((bhdp = (HEADER *)malloc(NODE)) == NULL)
      memerr();
   memset(bhdp, '\0', NODE);
   bhdp->keylength = len;
   bhdp->m = ((NODE - ((sizeof(int)*2) + (ADR*4)))/(len+ADR));
   bhdp->endnode = 1;
   remove(name);
   fp = fopen(name, "wb");
   fwrite(bhdp, NODE, 1, fp);
   fclose(fp);
   free(bhdp);
}

/* locate key in the btree */
RPTR locate(int tree, char *k)
{
   int i, fnd = FALSE;
   RPTR t, ad;
   char *a;

   trx = tree;
   t = bheader[trx].rootnode;
   if(t){
      read_node(t, &trnode);
      fnd = btreescan(&t, k, &a);
      ad = leaflevel(&t, &a, &i);
      if(i == trnode.keyct + 1){
         i = 0;
         t = trnode.rtsib;
      }
      currnode[trx] = t;
      currkno[trx] = i;
   }
   return (fnd ? ad : 0);
}

/* search tree */
static int btreescan(RPTR *t, char *k, char **a)
{
   int nl;
   do{
      if(nodescan(k,a)){
         while(compare_keys(*a, k) == FALSE)
            if(scanprev(t,a) == 0)
               break;
         if(compare_keys(*a, k))
            scannext(t,a);
         return TRUE;
      }
      nl = trnode.nonleaf;
      if(nl){
         *t = *((RPTR*)(*a - ADR));
         read_node(*t, &trnode);
      }
   } while(nl);
   return FALSE;
}

/* search node */
static int nodescan(char *keyvalue, char **nodeadr)
{
   int i;
   int result;

   *nodeadr = trnode.keyspace;
   for(i = 0; i < trnode.keyct; i++){
      result = compare_keys(keyvalue, *nodeadr);
      if(result == FALSE)
         return TRUE;
      if(result < 0)
         return FALSE;
      *nodeadr += ENTLN;
   }
   return FALSE;
}

/* compare keys */
static int compare_keys(char *a, char *b)
{
   int len = KLEN, cm;

   while(len--)
      if((cm = (int) *a++ - (int) *b++) != 0)
         break;
   return cm;
}

/* compute current file address */
static RPTR fileaddr(RPTR t, char *a)
{
   RPTR cn, ti;
   int i;

   ti = t;
   cn = leaflevel(&ti, &a, &i);
   read_node(t, &trnode);
   return cn;
}

/* navigate down to leaf level */
static RPTR leaflevel(RPTR *t, char **a, int *p)
{
   if(trnode.nonleaf == FALSE){ /* already at a leaf? */
      *p = (*a - trnode.keyspace) / ENTLN + 1;
      return *((RPTR*)(*a + KLEN));
   }
   *p = 0;
   *t = *((RPTR*)(*a+KLEN));
   read_node(*t, &trnode);
   *a = trnode.keyspace;
   while(trnode.nonleaf){
      *t = trnode.key0;
      read_node(*t, &trnode);
   }
   return trnode.key0;
}

/* delete a key */
int deletekey(int tree, char *x, RPTR ad)
{
   BTREE *qp, *yp;
   int rt_len, comb;
   RPTR p, adr, q, *b, y, z;
   char *a;

   trx = tree;
   if(trx >= MXTREES || fp[trx] == 0)
      return ERROR;
   p = bheader[trx].rootnode;
   if (p == 0)
      return OK;
   read_node(p, &trnode);
   if(btreescan(&p, x, &a) == FALSE)
      return OK;
   adr = fileaddr(p, a);
   while(adr != ad){
      adr = scannext(&p, &a);
      if(compare_keys(a,x))
         return OK;
   }
   if(trnode.nonleaf){
      b = (RPTR*)(a + KLEN);
      q = *b;
      if((qp = (BTREE*)malloc(NODE)) == NULL)
         memerr();
      read_node(q, qp);
      while(qp->nonleaf){
         q = qp->key0;
         read_node(q, qp);
      }
      /* move the left-mose key from the leaf
       * to where the deleted key is */
      memmove(a, qp->keyspace, KLEN);
      write_node(p, &trnode);
      p = q;
      trnode = *qp;
      a = trnode.keyspace;
      b = (RPTR*)(a + KLEN);
      trnode.key0 = *b;
      free(qp);
   }
   currnode[trx] = p;
   currkno[trx] = (a - trnode.keyspace) /ENTLN;
   rt_len = (trnode.keyspace + (bheader[trx].m * ENTLN)) - a;
   memmove(a, a + ENTLN, rt_len);
   memset(a + rt_len, '\0', ENTLN);
   trnode.keyct--;
   if(currkno[trx] > trnode.keyct){
      if(trnode.rtsib){
         currnode[trx] = trnode.rtsib;
         currkno[trx] = 0;
      }
      else
         currkno[trx]--;
   }
   while(trnode.keyct <= bheader[trx].m / 2
         && p!= bheader[trx].rootnode){
      comb = FALSE;
      z = trnode.prntnode;
      if((yp = (BTREE*)malloc(NODE)) == NULL)
         memerr();
      if(trnode.rtsib){
         y = trnode.rtsib;
         read_node(y, yp);
         if(yp->keyct + trnode.keyct < bheader[trx].m
               && yp->prntnode == z){
            comb = TRUE;
            implode(&trnode, yp);
         }
      }
      if(comb == FALSE && trnode.lfsib){
         y = trnode.lfsib;
         read_node(y, yp);
         if(yp->prntnode == z){
            if(yp->keyct + trnode.keyct < bheader[trx].m)
            {
               comb = TRUE;
               implode(yp, &trnode);
            }
            else {
               redist(yp, &trnode);
               write_node(p, &trnode);
               write_node(y, yp);
               free(yp);
               return OK;
            }
         }
      }
      if(comb == FLASE){
         y = trnode.rtsib;
         read_node(y, yp);
         redist(&trnode, yp);
         write_node(y, yp);
         write_node(p, &trnode);
         free(yp);
         return OK;
      }
      free(yp);
      p = z;
      read_node(p, &trnode);
   }
   if(trnode.keyct == 0){
      bheader[trx].rootnode = trnode.key0;
      trnode.non.eaf = FALSE;
      trnode.key0 = 0;
      trnode.prntnode = bheader[trx].rlsed_node;
      bheader[trx].rlsed_node = p;
   }
   if(bheader[trx].rootnode == 0)
      bheader[trx].rightmost = bheader[trx].leftmost = 0;
   write_node(p, &trnode);
   return OK;
}

/* combine two sibling nodes */
static void implode(BTREE *left, BTREE *right)
{
   RPTR lf, rt, p;
   int rt_len, lf_len;
   char *a;
   RPTR *b;
   BTREE *par;
   RPTR c;
   char *j;

   lf = right->lfsib;
   rt = left->rtsib;
   p = left->prntnode;
   if((par = (BTREE*)malloc(NODE)) == NULL)
      memerr();
   j = childptr(lf, p, par);
   /* move key from parent to end of left sibling */
   lf_len = left->keyct *ENTLN;
   a = left->keyspace + lf_len;
   memmove(a, j, KLEN);
   memset(j, '\0',  ENTLN);
   /* move keys from right sibling to left */
   b = (RPTR*)(a + KLEN);
   *b = right->key0;
   rt_len = right->keyct *ENTLN;
   a = (char*) (b + 1);
   memmove(a, right->keyspace, rt_len);
   /* point lower nodes to their new parent */
   if(left->nonleaf)
      adopt(b, right->keyct + 1, lf);
   /* if global key pointer -> to the right sibling,
    *                      change to -> left */
   if(currnode[trx] == left->rtsib){
      currnode[trx] = right->lfsib;
      currkno[trx] += left->keyct + 1;
   }
   /* update control vlues in left sibling node */
   left->keyct += right ->keyct + 1;
   c = bheader[trx].rlsed_node;
   bheader[trx].rlsed_node = left->rtsib;
   if(bheader[trx].rightmost == left->rtsib)
      bheader.rightmost = right->lfsib;
   left->rtsib = right->rtsib;
   /* point the deleted node's right brother
    * to this left brother */
   if(left->rtsib){
      read_node(left->rtsib, right);
      right->lfsib = lf;
      write_node(left->rtsib, right);
   }
   memset(right, '\0', NODE);
   right->prntnode = c;
   /* remove the key from the parent node */
   par->keyct--;
   if(par->keyct == 0)
      left->prntnode = 0;
   else{
      rt_len = par->keyspace + (par->keyct *ENTLN) - j;
      memmeove(j, j+ENTLN, rt_len);
   }
   write_node(lf, left);
   wirte_node(rt, right);
   write_node(p, par);
   free(par);
}

/* insert key */
int insertkey(int tree, char *x, RPTR ad, int unique)
{
   char k[MXKEYLEN + 1], *a;
   BTREE *yp;
   BTREE *bp;
   int nl_flag, rt_len, j;
   RPTR t,p,sv;
   RPTR *b;
   int lshft, rshft;

   trx = tree;
   if(trx >= MXTREES || fp[trx] == 0)
      return ERROR;
   p = 0;
   sv = 0;
   nl_flag = 0;
   memmove(k, x, KLEN);
   t = bheader[trx].rootnode;
   /* find insertion point */
   if(t){
      read_node(t, &trnode);
      if(btreescan(&t, k, &a)){
         if(unique)
            return ERROR;
         else {
            leaflevel(&t, &a, &j);
            currkno[trx] = j;
         }
      }
      else
         currkno[trx] = ((a - trnode.keyspace) / ENTLN) + 1;
      currnode[trx] = t;
   }
   /* insert key into leaf node */
   while(t){
      nl_flag = 1;
      rt_len = (trnode.keyspace + (bheader[trx].m * ENTLN)) - a;
      memmove(a+ENTLN, a, rt_len);
      memmove(a,k, KLEN);
      b = (RPTR*)(a + KLEN);
      *b = ad;
      if(trnode.nonleaf == FALSE){
         currnode[trx] = t;
         currkno[trx] = ((a - trnode.keyspace) / ENTLN ) + 1;
      }
      trnode.keyct++;
      if(trnode.keyct <= bheader[trx].m){
         write_node(t, &trnode);
         return OK;
      }
      /* redistribute keys between sibling nodes */
      lshft = FALSE;
      rshft = FALSE;
      if((yp = (BTREE*)malloc(NODE)) == NULL)
         memerr();
      if(trnode.lfsib){
         read_node(trnode.lfsib, yp);
         if(yp->keyct < bheader[trx].m
               && yp->prntnode == trnode.prntnode){
            lshft = TRUE;
            redist(yp, &trnode);
            write_node(trnode.lfsib, yp);
         }
      }
      if(lshft == FALSE && trnode.rtsib){
         read_node(trnode.rtsib, yp);
         if(yp->keyct < bheader[trx].m
               && yp->prntnode == trnode.prntnode){
            rshft = TRUE;
            redist(&trnode, yp);
            write_node(trnode.rtsib, yp);
         }
      }
      free(yp);
      if(lshft || rshft){
         write_node(t, &trnode);
         return OK;
      }
      p = nextnode();
      /* split node */
      if((bp = (BTREE*)malloc(NODE)) == NULL)
         memerr();
      memset(bp, '\0', NODE);
      trnode.keyct = (bheader[trx].m + 1) / 2;
      b = (RPTR*)
         (trnode.keyspace + ((trnode.keyct+1)*ENTLN)-ADR);
      bp->key0 = *b;
      bp->keyct = bheader[trx].m - trnode.keyct;
      rt_len = bp->keyct * ENTLN;
      a = (char*)(b+1);
      memmove(bp->keyspace, a, rt_len);
      bp->rtsib = trnode.rtsib;
      trnode.rtsib = p;
      bp->lfsib = t;
      bp->nonleaf = trnode.nonleaf;
      a -= ENTLN;
      memmove(k, a, KLEN);
      memset(a, '\0', rt_len + ENTLN);
      if(bheader[trx].rightmost == t)
         bheader[trx].rightmost = p;
      if(t == currnode[trx]
            && currkno[trx]>trnode.keyct){
         currnode[trx] = p;
         currkno[trx] -= trnode.keyct + 1;
      }
      ad = p;
      sv = t;
      t = trnode.prntnode;
      if(t)
         bp->prntnode = t;
      else {
         p = nextnode();
         trnode.prntnode = p;
         bp->prntnode = p;
      }
      write_node(ad, bp);
      if(bp->rtsib){
         if((yp = (BTREE*)malloc(NODE)) == NULL)
            memerr();
         read_node(bp->rtsib, yp);
         yp->lfsib = ad;
         write_node(bp->rtsib, yp);
         free(yp);
      }
      if(bp->nonleaf)
         adopt(&bp->key0, bp->keyct_1, ad);
      write_node(sv, &trnode);
      if(t){
         read_node(t, &trnode);
         a = trnode.keyxpa ;
         b - &trnode.key0;
         while(*b != bp->lfsib){
            a += ENTLN;
            b = (RPTR*)(a - ADR);
         }
      }
      free(bp);
   }
   /* new root */
   if (p == 0)
      p = nextnode();
   if((bp = (BTREE*)malloc(NODE)) == NULL)
      memerr();
   memset(bp, '\0', NODE);
   bp->nonleaf = nl_flag;
   bp->prntnode = 0;
   bp->rtsib = 0;
   bp->lfsib = 0;
   bp->keyct = 1;
   bp->key0 = sv;
   *((RPTR*)(bp->keyspace + KLEN)) = ad;
   memmove(bp->keyspace, k, KLEN);
   write_node(p, bp);
   free(bp);
   bheader[trx].rootnode = p;
   if(nl_flag == FALSE){
      bheader[trx].rightmost = p;
      bheader[trx].leftmost = p;
      currnode[trx] = p;
      currkno[trx] = 1;
   }
   return OK;
}

/* redistribute keys in sibling nodes */
static void redist(BTREE *left, BTREE *right)
{
   int n1, n2, len;
   RPTR z;
   char *c, *d, *e;
   BTREE *zp;

   n1 = (left->keyct + right->keyct) / 2;
   if(n1 == left->keyct)
      return;
   n2 = (left->keyct + right->keyct) - n1;
   z = left->prntnode;
   if((zp = (BTREE*)malloc(NODE)) == NULL)
      memerr();
   c = childptr(right->lfsib, z, zp);
   if(left->keyct < right->keyct){
      d = left->keyspace + (left->keyct * ENTLN);
      memmove(d, c, KLEN);
      d += KLEN;
      e = right->keyspace - ADR;
      len = ((right->keyct - n2 - 1) *ENTLN) + ADR;
      memmove(d,e,len);
      if(left->nonleaf)
         adopt(d, right->keyct - n2, right->lfsib);
      e += len;
      memmove(c, e, KLEN);
      e+= KLEN;
      d = right->keyspace - ADR;
      len = (n2 * ENTLN) + ADR;
      memmove(d, e, len);
      memset(d+len, '\0', e-d);
      if(right->nonleaf == 0
            && left->rtsib == currnode[trx])
         if(currno[trx] < right->keyct -n2){
            currnode[trx] = right->lfsib;
            currno[trx] += n1 + 1;
         }
         else
            currkno[trx] -= right->keyct - n2;
   }
   else {
      e = right->keyspace + ((n2 - right->keyct)*ENTLN)-ADR;
      memmove(e, right->keyspace - ADR,
                     (right-<keyct *ENTLN) + ADR);
      e -= KLEN;
      memmove(e, c, KLEN);
      d = left->keyspace + (n1 * ENTLN);
      memmove(c,d,KLEN);
      memset(d, '\0', KLEN);
      d += KLEN;
      len = ((left->keyct - n1 - 1) * ENTLN ) + ADR;
      memmove(right->keyspace - ADR, d, len);
      memset(d, '\0', len);
      if(right->nonleaf)
         adopt(right->keyspace - ADR,
               left->keyct - n1, left->rtsib);
      if(left->nonleaf == FALSE)
         if(right->lfsib == currnode[trx]
               && currkno[trx] > n1){
            currnode[trx] = left->rtsib;
            currkno[trx] -= n1 + 1;
         }
      else if(left->rtsib == currnode[trx])
         currkno[trx] += left->keyct - n1;
   }
   rith->keyct = n2;
   left->keyct = n1;
   write_node(z, zp);
   free(zp);
}

/* assign new parents to child nodes */
static void adopt(void *ad, int kct, RPTR newp)
{
   char *cp;
   BTREE *tmp;

   if((tmp = (BTREE*)malloc(NODE)) == NULL)
      memerr();
   while(kct--){
      read_node(*(RPTR*)ad, tmp);
      tmp->prntnode = newp;
      write_node(*(RPTR*)ad, tmp);
      cp = (char *)ad;
      cp += ENTLN;
      ad = cp;
   }
   free(tmp);
}

/* compute node address for a new node */
static RPTR nextnode(void)
{
   RPTR p;
   BTREE *nb;

   if(bheader[trx].rlsed_node) {
      if((nb = (BTREE*)malloc(NODE)) == NULL)
         memerr();
      p = bheader[trx].rlsed_node;
      read_node(p, nb);
      bheader[trx].rlsed_node = nb->prntnode;
      free(nb);
   }
   else
      p - bheadet[trx].endnode++;
   return p;
}

/* next squential key */
RPTR nextkey(int tree)
{
   trx = tree;
   if(currnode[trx] == 0)
      return firstkey(trx);
   read_node(currnode[trx], &trnode);
   if(currnkno[trx] == trnode.keyct){
      if(trnode.rtsib == 0){
         return 0;
      }
      currnode[trx] = trnode.rtsib;
      currkno[trx] = 0;
      read_node(trnode.rtsib, &trnode);
   }
   else
      currkno[trx]++;
   return *((RPTR*)
         (trnode.keyspace + (currkno[trx] * ENTLN) - ADR));
}

/* previous sequential key */
RPTR prevkey(int tree)
{
   trx = tree;
   if(currnode[trx] == 0)
      return lastkey(trx);
   read_node(currnode[trx], &trnode);
   if(currkno[trx] == 0){
      if(trnode.lfsib == 0)
         return 0;
      currnode[trx] = trnode.lfsib;
      read_node(trnode.lfsib, &trnode);
      currkno[trx] = trnode.keyct;
   }
   else
      currkno[trx]--;
   return *((RPTR *)
         (trnode.keyspace + (currkno[trx] *ENTLN) - ADR));
}

/* first key */
RPTR firstkey(int tree)
{
   trx = tree;
   if(bheader[trx].leftmost == 0)
      return 0;
   read_node(bheader[trx].leftmost, &trnode);
   currnode[trx] = bheader[trx].leftmost;
   currkno[trx] = 1;
   return *((RPTR*)(trnode.keyspace + KLEN));
}

/* last key */
RPTR lastkey(int tree)
{
   trx = tree;
   if(bheader[trx].rightmost == 0)
      return 0;
   read_node(bheader[trx].rightmost, &trnode);
   currnode[trx] = bheader[trx].rightmost;
   currkno[trx] = trnode.keyct;
   return *((RPTR*)
         (trnode.keyspace + (trnode.keyct *ENTLN) - ADR));
}

/* scan to the next sequential key */
static RPTR scannext(RPTR *p, char **a)
{
   RPTR cn;

   if(trnode.nonleaf){
      *p = *((RPTR*)(*a + KLEN));
      read_node(*p, &trnode);
      while(trnode.nonleaf){
         *p = trnode.key0;
         read_node(*p, &trnode);
      }
      *a = trnode.keyspace;
      return *((RPTR*)(*a+KLEN));
   }
   a+= ENTLN;
   while(-1){
      if((trnode.keyspace + (trnode.keyct) * ENTLN)
               != *a)
         return fileaddr(*p, *a);
      if(trnode.prntnode == 0 || trnode.rtsib == 0)
         break;
      cn = *p;
      *p = trnode.prntnode;
      read_node(*p &trnode);
      *a = trnode.keyspace;
      while(*((RPTR*)(*a-ADR)) != cn)
         *a += ENTLN;
   }
   return 0;
}

/* scan to the previous sequential key */
static RPTR scanprev(RPTR *p, char **a)
{
   RPTR cn;

   if(trnode.nonleaf){
      *p = *((RPTR*)(*a-ADR));
      read_node(*p, &trnode);
      while(trnode.nonlear){
         *p = *((RPTR*)
               (trnode.keyspace + (trnode.keyct)*ENTLN - ADR));
         read_node(*p, &trnode);
      }
      *a = trnode.keyspace + (trnode.keyct = 1) * ENTLN;
      return *((RPTR*)(*a + KLEN));
   }
   while(-1){
      if(trnode.keyspace != *a){
         *a -= ENTLN;
         return fileaddr(*p, *a);
      }
      if(trnode.prntnode == 0 || trnode.lfsib == 0)
         break;
      cn = *p;
      *p = trnode.prntnode;
      read_node(*p, &trnode);
      *a = trnode.keyspace;
      while(*((RPTR*)(*a - ADR)) != cn)
         *a += ENTLN;
   }
   return 0;
}

/* local pointer to child */
static char *childptr(RPTR left, RPTR parent, BTREE *btp)
{
   char *c;

   read_node(parent, btp);
   c = btp->keyspace;
   while(*((RPTR*)(c - ADR)) != left)
      c += ENTLN;
   return c;
}

/* currnet key value */
void keyval(int tree, char *ky)
{
   RPTR b, p;
   char *k;
   int i;

   trx = tree;
   b = currnode[trx];
   if(b){
      read_node(b, &trnode);
      i = currkno[trx];
      k = trnode.keyspace + ((i-1) * ENTLN);
      while(i == 0){
         b = p;
         b = trnode.prntnode;
         read_node(b, &trnode);
         for(; i <= trnode.keyct; i++){
            k = trnode.keyspace + ((i-1) * ENTLN);
            if(*((RPTR*)(k + KLEN)) == p)
               break;
         }
      }
      memmove(ky, k, KLEN);
   }
}

/* current key */
RPTR currkey(int tree)
{
   RPTR f = 0;

   trx = tree;
   if(currnode[trx]){
      read_node(currnode[trx], &trnode);
      f = *((RPTR*)
            (trnode.keyspace + (currkno[trx] * ENTLN) - ADR);




