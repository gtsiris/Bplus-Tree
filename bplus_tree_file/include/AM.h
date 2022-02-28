#ifndef AM_H_
#define AM_H_

#include "bf.h"
#include "defn.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

/* Error codes */

extern int AM_errno;

#define AME_OK 0
#define AME_EOF -1
#define AME_OPEN_FILES_LIMIT_ERROR -2
#define AME_INVALID_FILE_ERROR -3
#define AME_ACTIVE_ERROR -4
#define AME_FILE_ALREADY_EXISTS -5
#define AME_FULL_MEMORY_ERROR -6
#define AME_INVALID_BLOCK_NUMBER_ERROR -7
#define AME_ERROR -8
#define AME_ATTEMPT_TO_DESTROY_OPEN_FILE -9
#define AME_ATTEMPT_TO_CLOSE_FILE_UNDER_SCANNING -10
#define AME_NOT_BPLUS_FILE_FORMAT -11
#define AME_RUNNING_SCANS_LIMIT_ERROR -12
#define AME_ATTEMPT_TO_SCAN_NON_OPEN_FILE -13
#define AME_INVALID_KEY_TYPE -14
#define AME_NO_SUCH_ENTRY_IN_THIS_FILE -15
#define AME_ATTEMPT_TO_SCAN_EMPTY_FILE -16
#define AME_ATTEMPT_TO_CLOSE_NON_OPEN_FILE -17
#define AME_LENGTH_AND_TYPE_MISMATCH -18
#define AME_NO_SUCH_SCAN_TO_CLOSE -19
#define AME_NO_SUCH_SCAN_TO_PERFORM -20

#define EQUAL 1
#define NOT_EQUAL 2
#define LESS_THAN 3
#define GREATER_THAN 4
#define LESS_THAN_OR_EQUAL 5
#define GREATER_THAN_OR_EQUAL 6

#define MAX_OPEN_FILES 20  /* It must be less or equal to BF_MAX_OPEN_FILES */
#define MAX_SCANS 20
#define INDEX_BLOCK 0
#define LEAF_BLOCK 1
#define INVALID -1
#define EMPTY -2
#define NO_LINK -3

#define CALL_BF(call) {  \
    BF_ErrorCode bf_code = call; \
    int ame_code = BF_ERROR_CODE_TO_AME(bf_code);  \
    if (ame_code != AME_OK) {  \
        AM_errno = ame_code;  \
        return AM_errno;  \
    } \
}

/* Instead, function AM_FindNextEntry() uses the macro below */
#define CALL_BF_NULL(call) {  \
    BF_ErrorCode bf_code = call; \
    int ame_code = BF_ERROR_CODE_TO_AME(bf_code);  \
    if (ame_code != AME_OK) {  \
        AM_errno = ame_code;  \
        return NULL;  \
    } \
}


typedef struct Index_Entry_Type {
    void *key;
    int child;
} Index_Entry;

typedef struct Leaf_Entry_Type {
    void *key;
    void *value;
} Leaf_Entry;

typedef struct Info_Open_Type {
    int file_desc;
    char file_name[40];
} Info_Open;

typedef struct Info_Scan_Type {
    int file_desc;
    int op;
    void* value;
    int block_pos;
    int entry_pos;
} Info_Scan;

Info_Open open_files[MAX_OPEN_FILES];  /* A global array that contains info about all the currently open files */

Info_Scan running_scans[MAX_SCANS];  /* A global array that contains info about all the currently scanned files */



int BF_ERROR_CODE_TO_AME (BF_ErrorCode bf_error_code);  /* Translates the given BF error code to AME */


void AM_Init( void );


int AM_CreateIndex(
  char *fileName, /* όνομα αρχείου */
  char attrType1, /* τύπος πρώτου πεδίου: 'c' (συμβολοσειρά), 'i' (ακέραιος), 'f' (πραγματικός) */
  int attrLength1, /* μήκος πρώτου πεδίου: 4 γιά 'i' ή 'f', 1-255 γιά 'c' */
  char attrType2, /* τύπος πρώτου πεδίου: 'c' (συμβολοσειρά), 'i' (ακέραιος), 'f' (πραγματικός) */
  int attrLength2 /* μήκος δεύτερου πεδίου: 4 γιά 'i' ή 'f', 1-255 γιά 'c' */
);


int AM_DestroyIndex(
  char *fileName /* όνομα αρχείου */
);


int AM_OpenIndex (
  char *fileName /* όνομα αρχείου */
);


int AM_CloseIndex (
  int indexDesc /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
);


int AM_InsertEntry(
  int indexDesc, /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
  void *value1, /* τιμή του πεδίου-κλειδιού προς εισαγωγή */
  void *value2 /* τιμή του δεύτερου πεδίου της εγγραφής προς εισαγωγή */
);


int AM_OpenIndexScan(
  int indexDesc, /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
  int op, /* τελεστής σύγκρισης */
  void *value /* τιμή του πεδίου-κλειδιού προς σύγκριση */
);


void *AM_FindNextEntry(
  int scanDesc /* αριθμός που αντιστοιχεί στην ανοιχτή σάρωση */
);


int AM_CloseIndexScan(
  int scanDesc /* αριθμός που αντιστοιχεί στην ανοιχτή σάρωση */
);


void AM_PrintError(
  char *errString /* κείμενο για εκτύπωση */
);

void AM_Close();


#endif /* AM_H_ */
