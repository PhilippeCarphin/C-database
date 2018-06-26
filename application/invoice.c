/* ---------------invoice.c --------------------- */

/*
 * Produce invoices from the clients file
 */

#include <stdio.h>
#include <stdlib.h>
#include "cbs.h"

struct clients cl;

void main(void)
{
   static DBFILE fl[] = {CLIENTS, -1};

   db_open("", fl);
   while(TRUE){
      if(next_rcd(CLIENTS, 1, &cl) == ERROR)
         break;
      printf("\n\n\nInvoice for Services Rendered\n");
      printf("\n%s", cl.client_name);
      printf("\n%s", cl.address);
      printf("\n%s, %s %s", cl.city, cl.state, cl.zip);
      printf("\n\nAmount Due: $%ld,%02ld\n",
            atol(cl.amt_due) / 100,
            atol(cl.amt_due) % 100);
   }
   db_cls();
}
