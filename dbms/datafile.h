/* ------------------ datafile.h ------------------- */

/* ----------------- data file prototypes ------------------- */
void file_create(char *, int);
int file_open(char *);
void file_close(int);
RPTR new_record(int, void *);
int get_record(int, RPTR, void *);
int put_record(int, RPTR, void *);
int delete_record(int, RPTR);

/* ---------------- file header ------------------- */
typedef struct fhdr {
   RPTR first_record;
   RPTR next_record;
   int record_length;
} FHEADER;
