#include "AM.h"

int AM_errno = AME_OK;

void AM_Init() {
    BF_Init(LRU);
    for (int i = 0; i <= MAX_OPEN_FILES - 1; i++) {
        open_files[i].file_desc = EMPTY;  /* Initialize each slot of the global array as empty */
    }
    for (int j = 0; j <= MAX_SCANS - 1; j++) {
        running_scans[j].file_desc = EMPTY;  /* Initialize each slot of the global array as empty */
    }
}

int AM_CreateIndex(char *fileName, char attrType1, int attrLength1, char attrType2, int attrLength2) {
    if (attrType1 == INTEGER && attrLength1 != sizeof(int) \
    || attrType1 == FLOAT && attrLength1 != sizeof(float) \
    || attrType1 == STRING && (attrLength1 < 1 || attrLength1 > 255)) {  /* If the given lengths don't match the given types, then error */
        AM_errno = AME_LENGTH_AND_TYPE_MISMATCH;
        return AM_errno;
    }
    
    CALL_BF(BF_CreateFile(fileName));  /* Create a file using BF level */
    int fileDesc;
    CALL_BF(BF_OpenFile(fileName, &fileDesc));  /* Open the file and set fileDesc to a valid value using BF level */
    
    BF_Block *info_block;
    BF_Block_Init(&info_block);
    CALL_BF(BF_AllocateBlock(fileDesc, info_block));  /* Allocate memory for metadata block */
    char *file_type = BF_Block_GetData(info_block);
    char *string = "bplus";
    memcpy(file_type, string, strlen(string) + 1);  /* Set the type of the file */
    char *key_type = file_type + strlen(file_type) + 1;
    *key_type = attrType1;
    int *key_length = (int *)(key_type + 1);
    *key_length = attrLength1;
    char *value_type = (char *)(key_length + 1);
    *value_type = attrType2;
    int *value_length = (int *)(value_type + 1);
    *value_length = attrLength2;
    int *root_pos = value_length + 1;  /* The position of the root block */
    *root_pos = 1;  /* Initially it is 1 (0 is the info block), but later it might change (performing split) */
    BF_Block_SetDirty(info_block);
    CALL_BF(BF_UnpinBlock(info_block));
    BF_Block_Destroy(&info_block);
    
    BF_Block *root_block;
    BF_Block_Init(&root_block);
    CALL_BF(BF_AllocateBlock(fileDesc, root_block));
    bool *status = (bool *)BF_Block_GetData(root_block);
    *status = INDEX_BLOCK;
    int *entry_counter = (int *)(status + 1);
    *entry_counter = 0;  /* Initially empty */
    BF_Block_SetDirty(root_block);
    CALL_BF(BF_UnpinBlock(root_block));
    BF_Block_Destroy(&root_block);
    
    CALL_BF(BF_CloseFile(fileDesc));  /* Close the file using BF level */
    return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
    for (int i = 0; i <= MAX_OPEN_FILES - 1; i++) {
        if (strcmp(open_files[i].file_name, fileName) == 0) {  /* If it is open, then it cannot be removed */
            AM_errno = AME_ATTEMPT_TO_DESTROY_OPEN_FILE;
            return AM_errno;
        }
    }
    remove(fileName);  /* Remove file from the disk */
    return AME_OK;
}


int AM_OpenIndex (char *fileName) {
  int indexDesc = INVALID;  /* Index in the global array open_files */
    for (int i = 0; i <= MAX_OPEN_FILES - 1; i++) {
        if (open_files[i].file_desc == EMPTY) {  /* If an empty slot is found */
            indexDesc = i;  /* Use it */
            break;
        }
    }
    if (indexDesc == INVALID) {  /* There wasn't any empty slot */
        AM_errno = AME_OPEN_FILES_LIMIT_ERROR;  /* The maximum number of open files has been reached */
        return AM_errno;
    }
    
    int fileDesc;
  CALL_BF(BF_OpenFile(fileName, &fileDesc));  /* Open the file and set fileDesc to a valid value using BF level */
    open_files[indexDesc].file_desc = fileDesc;  /* Update the global array's info */
    strcpy(open_files[indexDesc].file_name, fileName);
    
    BF_Block *info_block;
  BF_Block_Init(&info_block);
  CALL_BF(BF_GetBlock(fileDesc, 0, info_block));
  char *file_type = BF_Block_GetData(info_block);
  if (strcmp(file_type, "bplus") != 0) {
        AM_errno = AME_NOT_BPLUS_FILE_FORMAT;
        CALL_BF(BF_UnpinBlock(info_block));
    BF_Block_Destroy(&info_block);
  	return AM_errno;
  }
  CALL_BF(BF_UnpinBlock(info_block));
  BF_Block_Destroy(&info_block);
    
  return indexDesc;
}


int AM_CloseIndex (int indexDesc) {
    if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES - 1 || open_files[indexDesc].file_desc == EMPTY) {  /* Check if given indexDesc is valid */
        AM_errno = AME_ATTEMPT_TO_CLOSE_NON_OPEN_FILE;
        return AM_errno;
    }
    int fileDesc = open_files[indexDesc].file_desc;  /* Get the file descriptor using the global array */
    for (int j = 0; j <= MAX_SCANS - 1; j++) {
        if (running_scans[j].file_desc == fileDesc) {  /* If the fileDesc is found in the running_scans, then error */
            AM_errno = AME_ATTEMPT_TO_CLOSE_FILE_UNDER_SCANNING;
            return AM_errno;
        }
    }
    CALL_BF(BF_CloseFile(fileDesc));  /* Close the file using BF level */
    open_files[indexDesc].file_desc = EMPTY;  /* Mark this slot as empty */
    strcpy(open_files[indexDesc].file_name, "");
    return AME_OK;
}


int AM_InsertEntry(int indexDesc, void *value1, void *value2) {  /* Doesn't work properly */
    /* Check whether there is enough space to insert the entry to an already existing block (scan will show which one) */
    /* If the space is not enough, perform split */
    /*
    if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES - 1 || open_files[indexDesc].file_desc == EMPTY) {
        AM_errno = AME_ATTEMPT_TO_INSERT_TO_NON_OPEN_FILE;
        return AM_errno;
    }
    int fileDesc = open_files[indexDesc].file_desc;
    BF_Block *info_block;
  BF_Block_Init(&info_block);
  CALL_BF(BF_GetBlock(fileDesc, 0, info_block));
  char *file_type = BF_Block_GetData(info_block);
  if (strcmp(file_type, "bplus") != 0) {
        AM_errno = AME_NOT_BPLUS_FILE_FORMAT;
        CALL_BF(BF_UnpinBlock(info_block));
    BF_Block_Destroy(&info_block);
  	return AM_errno;
  }
  char *key_type = file_type + strlen(file_type) + 1;
    int *key_length = (int *)(key_type + 1);
    char *value_type = (char *)(key_length + 1);
    int *value_length = (int *)(value_type + 1);
    int *root_pos = value_length + 1;
    
    BF_Block *root_block;
    BF_Block_Init(&root_block);
    CALL_BF(BF_GetBlock(fileDesc, *root_pos, root_block));
    bool *status = (bool *)BF_Block_GetData(root_block);
    int *entry_counter = (int *)(status + 1);
    int *first_child = entry_counter + 1;
    Index_Entry *index_entries = (Index_Entry *)(first_child + 1);
    if (*entry_counter == 0) {  // first insertion to this file
        *entry_counter++;
        *first_child = NO_LINK;
        index_entries[0].key = value1;
        index_entries[0].child = 2;
        BF_Block_SetDirty(root_block);
        CALL_BF(BF_UnpinBlock(root_block));
        BF_Block_Destroy(&root_block);
        
        BF_Block *new_block;
        BF_Block_Init(&new_block);
        CALL_BF(BF_AllocateBlock(fileDesc, new_block));
        bool *status = (bool *)BF_Block_GetData(new_block);
        *status = LEAF_BLOCK;
        int *entry_counter = (int *)(status + 1);
        *entry_counter = 1;
        int *next_leaf_block = entry_counter + 1;
        *next_leaf_block = NO_LINK;
        Leaf_Entry *leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);
        leaf_entries[0].key = value1;
        leaf_entries[0].value = value2;
        BF_Block_SetDirty(new_block);
        CALL_BF(BF_UnpinBlock(new_block));
        BF_Block_Destroy(&new_block);
        CALL_BF(BF_UnpinBlock(info_block));
        BF_Block_Destroy(&info_block);
        return AME_OK;
    }
    
    AM_OpenIndexScan(indexDesc, EQUAL, value1);
    int scanDesc = INVALID;
    for (int j = 0; j <= MAX_SCANS - 1; j++) {
        if (*key_type == INTEGER) {
            if (running_scans[j].file_desc == fileDesc && running_scans[j].op == EQUAL && *(int *)running_scans[j].value == *(int *)value1) {
                scanDesc = j;
                break;
            }
        }
        else if (*key_type == FLOAT) {
            if (running_scans[j].file_desc == fileDesc && running_scans[j].op == EQUAL && *(float *)running_scans[j].value == *(float *)value1) {
                scanDesc = j;
                break;
            }
        }
        else if (*key_type == STRING) {
            if (running_scans[j].file_desc == fileDesc && running_scans[j].op == EQUAL && strcmp((char *)running_scans[j].value, (char *)value1) == 0) {
                scanDesc = j;
                break;
            }
        }
    }
    if (scanDesc == INVALID) {
        AM_errno = AME_RUNNING_SCANS_LIMIT_ERROR;
        CALL_BF(BF_UnpinBlock(root_block));
        BF_Block_Destroy(&root_block);
        CALL_BF(BF_UnpinBlock(info_block));
        BF_Block_Destroy(&info_block);
        return AM_errno;
    }
    int block_pos = running_scans[scanDesc].block_pos;
    BF_Block *block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
    bool *status = (bool *)BF_Block_GetData(block);
    int *entry_counter = (int *)(status + 1);
    int *next_leaf_block = entry_counter + 1;
    Leaf_Entry *leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);
    int entry = 0;
    for (; entry <= *entry_counter - 1; entry++) {
        if (value1 > leaf_entries[entry].key) {
            break;
        }
    }
    if (sizeof(bool) + 2 * sizeof(int) + (*entry_counter + 1) * sizeof(Leaf_Entry) <= BF_BLOCK_SIZE) {
        for (int entry_shift = *entry_counter - 1; entry_shift >= entry; entry_shift--) {
            leaf_entries[entry_shift + 1].key = leaf_entries[entry_shift].key;
            leaf_entries[entry_shift + 1].value = leaf_entries[entry_shift].value;
        }
        leaf_entries[entry].key = value1;
        leaf_entries[entry].value = value2;
        (*entry_counter)++;
    }
    else {
        int half_entries = (*entry_counter + 1) / 2;
        BF_Block *new_block;
        BF_Block_Init(&new_block);
        CALL_BF(BF_AllocateBlock(fileDesc, new_block));
        bool *status_new = (bool *)BF_Block_GetData(new_block);
        *status_new = LEAF_BLOCK;
        int *entry_counter_new = (int *)(status_new + 1);
        int *next_leaf_block_new = entry_counter_new + 1;
        *next_leaf_block_new = *next_leaf_block;
        int blocks_num;
        CALL_BF(BF_GetBlockCounter(fileDesc, &blocks_num));
        *next_leaf_block = blocks_num - 1;
        Leaf_Entry *leaf_entries_new = (Leaf_Entry *)(next_leaf_block_new + 1);
        if (entry < half_entries) {
            for (int entry_shift = half_entries - 1; entry_shift >= entry; entry_shift--) {
                leaf_entries[entry_shift + 1].key = leaf_entries[entry_shift].key;
                leaf_entries[entry_shift + 1].value = leaf_entries[entry_shift].value;
            }
            leaf_entries[entry].key = value1;
            leaf_entries[entry].value = value2;
            (*entry_counter)++;
        }
        else {
            int entry_move = half_entries - 1;
            for (; entry_move <= *entry_counter - 1; entry_move++) {
                if (entry_move == entry) {
                    leaf_entries_new[entry_move - half_entries - 1].key = value1;
                    leaf_entries_new[entry_move - half_entries - 1].value = value2;
                    break;
                }
                leaf_entries_new[entry_move - half_entries - 1].key = leaf_entries[entry_move].key;
                leaf_entries_new[entry_move - half_entries - 1].value = leaf_entries[entry_move].value;
            }
            for (; entry_move <= *entry_counter - 1; entry_move++) {
                leaf_entries_new[entry_move - half_entries].key = leaf_entries[entry_move].key;
                leaf_entries_new[entry_move - half_entries].value = leaf_entries[entry_move].value;
            }
        }
        BF_Block_SetDirty(new_block);
        CALL_BF(BF_UnpinBlock(new_block));
        BF_Block_Destroy(&new_block);
    }
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    CALL_BF(BF_UnpinBlock(root_block));
    BF_Block_Destroy(&root_block);
    CALL_BF(BF_UnpinBlock(info_block));
    BF_Block_Destroy(&info_block);
    */
  return AME_OK;
}


int AM_OpenIndexScan(int indexDesc, int op, void *value) {
    if (indexDesc < 0 || indexDesc > MAX_OPEN_FILES - 1 || open_files[indexDesc].file_desc == EMPTY) {  /* Check if given indexDesc is valid */
        AM_errno = AME_ATTEMPT_TO_SCAN_NON_OPEN_FILE;
        return AM_errno;
    }
    int fileDesc = open_files[indexDesc].file_desc;  /* Get the file descriptor using the global array */
    char fileName[40];
    strcpy(fileName, open_files[indexDesc].file_name);  /* Get the file name using the global array */
    
    int scanDesc = INVALID;  /* Index in the global array runnning_scans */
    for (int j = 0; j <= MAX_SCANS - 1; j++) {
        if (running_scans[j].file_desc == EMPTY) { /* If an empty slot is found */
            scanDesc = j;  /* Use it */
            break;
        }
    }
    if (scanDesc == INVALID) {  /* There wasn't any empty slot */
        AM_errno = AME_RUNNING_SCANS_LIMIT_ERROR;  /* The maximum number of running scans has been reached */
        return AM_errno;
    }
    
    BF_Block *info_block;
  BF_Block_Init(&info_block);
  CALL_BF(BF_GetBlock(fileDesc, 0, info_block));
  char *file_type = BF_Block_GetData(info_block);
  if (strcmp(file_type, "bplus") != 0) {
        AM_errno = AME_NOT_BPLUS_FILE_FORMAT;
        CALL_BF(BF_UnpinBlock(info_block));
    BF_Block_Destroy(&info_block);
  	return AM_errno;
  }
  char *key_type = file_type + strlen(file_type) + 1;
    int *key_length = (int *)(key_type + 1);
    char *value_type = (char *)(key_length + 1);
    int *value_length = (int *)(value_type + 1);
    int *root_pos = value_length + 1;
    
    BF_Block *block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fileDesc, *root_pos, block));
    bool *status = (bool *)BF_Block_GetData(block);
    int *entry_counter = (int *)(status + 1);
    int *first_child = entry_counter + 1;  /* first_child shows the block with less values than the key of the first entry */
    Index_Entry *index_entries = (Index_Entry *)(first_child + 1);  /* The entries of the index block */
    if (*entry_counter == 0) {  /* If there isn't any entry */
        AM_errno = AME_ATTEMPT_TO_SCAN_EMPTY_FILE;
        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
        CALL_BF(BF_UnpinBlock(info_block));
        BF_Block_Destroy(&info_block);
        return AM_errno;
    }
    int block_pos = INVALID;  /* This will show in which block the first desired entry is located */
    int entry_pos = INVALID;  /* This will show in which pos inside the block the first desired entry is located */
    int *next_leaf_block;  /* The following leaf block */
    Leaf_Entry *leaf_entries;  /* The entries of the leaf block */
    if (*key_type == INTEGER) {  /* The keys are integers */
        switch (op) {
            case EQUAL:  /* Looking for equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (*(int *)value < *(int *)(index_entries[entry].key)) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(int *)value == *(int *)(leaf_entries[entry].key)) {  /* If the value is equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else if (*(int *)value > *(int *)(leaf_entries[entry].key)) {  /* If the value is greater than its key */
                            break;  /* An equal key is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || *(int *)value > *(int *)(leaf_entries[entry].key)) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case NOT_EQUAL:  /* Looking for not equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(int *)value != *(int *)(leaf_entries[entry].key)) {  /* If the value is not equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case LESS_THAN:  /* Looking for less than the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(int *)value < *(int *)(leaf_entries[entry].key)) {  /* If the value is less than its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else {  /* If the value is greater than or equal to its key */
                            break;  /* A key like that is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || *(int *)value >= *(int *)(leaf_entries[entry].key)) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case GREATER_THAN:  /* Looking for greater than the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (*(int *)value < *(int *)(index_entries[entry].key)) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(int *)value > *(int *)(leaf_entries[entry].key)) {  /* If the value is greater than its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case LESS_THAN_OR_EQUAL:  /* Looking for less than or equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(int *)value <= *(int *)(leaf_entries[entry].key)) {  /* If the value is less than or equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else {  /* If the value is greater than its key */
                            break;  /* A key like that is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || *(int *)value > *(int *)(leaf_entries[entry].key)) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case GREATER_THAN_OR_EQUAL:  /* Looking for greater than or equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (*(int *)value < *(int *)(index_entries[entry].key)) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(int *)value >= *(int *)(leaf_entries[entry].key)) {  /* If the value is greater than or equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
        }
    }
    
    else if (*key_type == FLOAT) {  /* The keys are floats */
        switch (op) {
            case EQUAL:  /* Looking for equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (*(float *)value < *(float *)(index_entries[entry].key)) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(float *)value == *(float *)(leaf_entries[entry].key)) {  /* If the value is equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else if (*(float *)value > *(float *)(leaf_entries[entry].key)) {  /* If the value is greater than its key */
                            break;  /* An equal key is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || *(float *)value > *(float *)(leaf_entries[entry].key)) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case NOT_EQUAL:  /* Looking for not equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(float *)value != *(float *)(leaf_entries[entry].key)) {  /* If the value is not equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case LESS_THAN:  /* Looking for less than the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(float *)value < *(float *)(leaf_entries[entry].key)) {  /* If the value is less than its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else {  /* If the value is greater than or equal to its key */
                            break;  /* A key like that is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || *(float *)value >= *(float *)(leaf_entries[entry].key)) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case GREATER_THAN:  /* Looking for greater than the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (*(float *)value < *(float *)(index_entries[entry].key)) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(float *)value > *(float *)(leaf_entries[entry].key)) {  /* If the value is greater than its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case LESS_THAN_OR_EQUAL:  /* Looking for less than or equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(float *)value <= *(float *)(leaf_entries[entry].key)) {  /* If the value is less than or equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else {  /* If the value is greater than its key */
                            break;  /* A key like that is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || *(float *)value > *(float *)(leaf_entries[entry].key)) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case GREATER_THAN_OR_EQUAL:  /* Looking for greater than or equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (*(float *)value < *(float *)(index_entries[entry].key)) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (*(float *)value >= *(float *)(leaf_entries[entry].key)) {  /* If the value is greater than or equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
        }
    }
    
    else if (*key_type == STRING) {  /* The keys are strings */
        switch (op) {
            case EQUAL:  /* Looking for equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (strcmp((char *)value, (char *)(index_entries[entry].key)) < 0) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (strcmp((char *)value, (char *)(leaf_entries[entry].key)) == 0) {  /* If the value is equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else if (strcmp((char *)value, (char *)(leaf_entries[entry].key)) > 0) {  /* If the value is greater than its key */
                            break;  /* An equal key is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || strcmp((char *)value, (char *)(leaf_entries[entry].key)) > 0) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case NOT_EQUAL:  /* Looking for not equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (strcmp((char *)value, (char *)(leaf_entries[entry].key)) != 0) {  /* If the value is not equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case LESS_THAN:  /* Looking for less than the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (strcmp((char *)value, (char *)(leaf_entries[entry].key)) < 0) {  /* If the value is less than its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else {  /* If the value is greater than or equal to its key */
                            break;  /* A key like that is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || strcmp((char *)value, (char *)(leaf_entries[entry].key)) >= 0) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case GREATER_THAN:  /* Looking for greater than the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (strcmp((char *)value, (char *)(index_entries[entry].key)) < 0) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (strcmp((char *)value, (char *)(leaf_entries[entry].key)) > 0) {  /* If the value is greater than its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case LESS_THAN_OR_EQUAL:  /* Looking for less than or equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    block_pos = *first_child;  /* Repeatedly choose the first_child in order to reach the leftmost leaf block */
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    int entry = 0;  /* Starting from the entry with the smallest key (they are sorted) */
                    for (; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (strcmp((char *)value, (char *)(leaf_entries[entry].key)) <= 0) {  /* If the value is less than or equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                        else {  /* If the value is greater than its key */
                            break;  /* A key like that is not going to be found (they are sorted) */
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK || strcmp((char *)value, (char *)(leaf_entries[entry].key)) > 0) {
                        break;  /* Stop if the position is found, or it isn't going to be found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
            case GREATER_THAN_OR_EQUAL:  /* Looking for greater than or equal to the value */
                while (*status != LEAF_BLOCK) {  /* Until a leaf block is reached */
                    first_child = entry_counter + 1;  /* Update */
                    index_entries = (Index_Entry *)(first_child + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry*/
                        if (strcmp((char *)value, (char *)(index_entries[entry].key)) < 0) {  /* Check if the value is less than its key */
                            if (entry > 0) {  /* If it isn't the first entry */
                                block_pos = index_entries[entry - 1].child;  /* Then choose the child of the previous entry */
                            }
                            else if (entry == 0) {  /* Else, there isn't a previous entry */
                                block_pos = *first_child;  /* So choose the first_child */
                            }
                            break;
                        }
                        else if (entry == *entry_counter - 1) {  /* If the value is greater than or equal to the key of the last entry */
                            block_pos = index_entries[entry].child;  /* Choose its child */
                            break;
                        }
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                while (1) {  /* Now lets find which is the position of the first desired entry in this block */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                    for (int entry = 0; entry <= *entry_counter - 1; entry++) {  /* For each entry in this block */
                        if (strcmp((char *)value, (char *)(leaf_entries[entry].key)) >= 0) {  /* If the value is greater than or equal to its key */
                            entry_pos = entry;  /* Save the position */
                            break;
                        }
                    }
                    if (entry_pos != INVALID || *next_leaf_block == NO_LINK) {
                        break;  /* Stop if the position is found, or there isn't next leaf block */
                    }
                    CALL_BF(BF_UnpinBlock(block));
                    CALL_BF(BF_GetBlock(fileDesc, *next_leaf_block, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                }
                break;
        }
    }
    
    else { /* Invalid key type error*/
        AM_errno = AME_INVALID_KEY_TYPE;
        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
  	CALL_BF(BF_UnpinBlock(info_block));
  	BF_Block_Destroy(&info_block);
        return AM_errno;
    }
    
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    CALL_BF(BF_UnpinBlock(info_block));
    BF_Block_Destroy(&info_block);
    
    if (entry_pos == INVALID) {  /* If there wasn't any entry with the desired key */
        AM_errno = AME_NO_SUCH_ENTRY_IN_THIS_FILE;
        return AM_errno;
    }
    running_scans[scanDesc].file_desc = fileDesc;  /* Update the global array's info */
    running_scans[scanDesc].op = op;
    running_scans[scanDesc].value = value;
    running_scans[scanDesc].block_pos = block_pos;
    running_scans[scanDesc].entry_pos = entry_pos;
  return scanDesc;  /* Return the used position in the array */
}


void *AM_FindNextEntry(int scanDesc) {
    if (scanDesc < 0 || scanDesc > MAX_OPEN_FILES - 1 || running_scans[scanDesc].file_desc == EMPTY) {  /* Check if given scanDesc is valid */
        AM_errno = AME_NO_SUCH_SCAN_TO_PERFORM;
        return NULL;
    }
    int fileDesc = running_scans[scanDesc].file_desc;  /* Get the file descriptor using the global array */
    int op = running_scans[scanDesc].op;  /* Get the operator using the global array */
    void* value = running_scans[scanDesc].value;  /* Get the value using the global array */
    int block_pos = running_scans[scanDesc].block_pos;  /* Get the block position using the global array */
    int entry_pos = running_scans[scanDesc].entry_pos;  /* Get the entry position using the global array */
    if (entry_pos == INVALID) {  /* If there isn't any other entry with the desired key */
        AM_errno = AME_EOF;
        return NULL;
    }
    BF_Block *block;
    BF_Block_Init(&block);
    CALL_BF_NULL(BF_GetBlock(fileDesc, block_pos, block));
    bool *status = (bool *)BF_Block_GetData(block);
    int *entry_counter = (int *)(status + 1);
    int *next_leaf_block = entry_counter + 1;
    Leaf_Entry *leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);
    
    void* requested_value = leaf_entries[entry_pos].value;
    
    /* Now lets find the next desired entry (if it exists) to update the block_pos and the entry_pos */
    if (entry_pos < *entry_counter - 1) {  /* If it isn't the last entry of the block */
        entry_pos++;  /* Then the next one is right after it */
    }
    else if (entry_pos == *entry_counter - 1) {  /* Else if it is the last entry of the block */
        if (*next_leaf_block != NO_LINK) {  /* If there is a next leaf block */
            block_pos = *next_leaf_block;  /* The next desired entry will be there */
            entry_pos = 0;  /* And it is going to be the first entry of this block */
            CALL_BF_NULL(BF_UnpinBlock(block));
            CALL_BF_NULL(BF_GetBlock(fileDesc, block_pos, block));
            status = (bool *)BF_Block_GetData(block);  /* Update */
            entry_counter = (int *)(status + 1);  /* Update */
            next_leaf_block = entry_counter + 1;  /* Update */
            leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
        }
        else {  /* If there isn't next leaf block */
            entry_pos = INVALID;  /* Mark entry_pos as invalid to indicate EOF */
        }
    }
    
    BF_Block *info_block;
  BF_Block_Init(&info_block);
  CALL_BF_NULL(BF_GetBlock(fileDesc, 0, info_block));
  char *file_type = BF_Block_GetData(info_block);
  if (strcmp(file_type, "bplus") != 0) {
        AM_errno = AME_NOT_BPLUS_FILE_FORMAT;
        CALL_BF_NULL(BF_UnpinBlock(info_block));
    BF_Block_Destroy(&info_block);
  	return NULL;
  }
  char *key_type = file_type + strlen(file_type) + 1;
    int *key_length = (int *)(key_type + 1);
    char *value_type = (char *)(key_length + 1);
    int *value_length = (int *)(value_type + 1);
    int *root_pos = value_length + 1;
    
    if (*key_type == INTEGER) {  /* The keys are integers */
        switch (op) {
            case EQUAL:  /* Looking for equal to the value */
                if (*(int *)leaf_entries[entry_pos].key == *(int *)value) {  /* If the key of the next entry that we found is equal to the value */
                    break;  /* Done */
                }
                else {  /* If it is greater (cannot be less, because they are sorted) */
                    entry_pos = INVALID;  /* An equal key is not going to be found (they are sorted) */
                }
                break;
            case NOT_EQUAL:  /* Looking for not equal to the value */
                while(1) {
                    for (; entry_pos <= *entry_counter - 1; entry_pos++) {  /* For each following entry of the current block */
                        if (*(int *)leaf_entries[entry_pos].key != *(int *)value) {  /* If its key is not equal to the value */
                            break;  /* Done */
                        }
                    }
                    if (*(int *)leaf_entries[entry_pos].key != *(int *)value) {  /* If its key is not equal to the value */
                        break;  /* Done */
                    }
                    if (*next_leaf_block == NO_LINK) {  /* If a non-equal key wasn't found and there isn't a next leaf block */
                        entry_pos = INVALID;  /* Mark entry_pos as invalid to indicate EOF */
                        break;
                    }
                    block_pos = *next_leaf_block;  /* Go to the next leaf block */
                    entry_pos = 0;  /* Starting from its first entry */
                    CALL_BF_NULL(BF_UnpinBlock(block));
                    CALL_BF_NULL(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                }
                break;
            case LESS_THAN:  /* Looking for less than the value */
                if (*(int *)leaf_entries[entry_pos].key < *(int *)value) {  /* If the key of the next entry that we found is less than the value */
                    break;  /* Done */
                }
                else {  /* If it is greater or equal */
                    entry_pos = INVALID;  /* A smaller key is not going to be found (they are sorted) */
                }
                break;
            case GREATER_THAN:  /* Looking for greater than the value */
                break;  /* Since the previous entry had a greater key the same applies to the next one that we found (because they are sorted) */
            case LESS_THAN_OR_EQUAL:  /* Looking for less than or equal to the value */
                if (*(int *)leaf_entries[entry_pos].key <= *(int *)value) {  /* If the key of the next entry that we found is less than or equal to the value */
                    break;  /* Done */
                }
                else {  /* If it is greater */
                    entry_pos = INVALID;  /* A smaller key is not going to be found (they are sorted) */
                }
                break;
            case GREATER_THAN_OR_EQUAL:  /* Looking for greater than or equal to the value */
                break;  /* Since the previous entry had a greater or equal key the same applies to the next one that we found (because they are sorted) */
        }
    }
    
    else if (*key_type == FLOAT) {  /* The keys are floats */
        switch (op) {
            case EQUAL:  /* Looking for equal to the value */
                if (*(float *)leaf_entries[entry_pos].key == *(float *)value) {  /* If the key of the next entry that we found is equal to the value */
                    break;  /* Done */
                }
                else {  /* If it is greater (cannot be less, because they are sorted) */
                    entry_pos = INVALID;  /* An equal key is not going to be found (they are sorted) */
                }
                break;
            case NOT_EQUAL:  /* Looking for not equal to the value */
                while(1) {
                    for (; entry_pos <= *entry_counter - 1; entry_pos++) {  /* For each following entry of the current block */
                        if (*(float *)leaf_entries[entry_pos].key != *(float *)value) {  /* If its key is not equal to the value */
                            break;  /* Done */
                        }
                    }
                    if (*(float *)leaf_entries[entry_pos].key != *(float *)value) {  /* If its key is not equal to the value */
                        break;  /* Done */
                    }
                    if (*next_leaf_block == NO_LINK) {  /* If a non-equal key wasn't found and there isn't a next leaf block */
                        entry_pos = INVALID;  /* Mark entry_pos as invalid to indicate EOF */
                        break;
                    }
                    block_pos = *next_leaf_block;  /* Go to the next leaf block */
                    entry_pos = 0;  /* Starting from its first entry */
                    CALL_BF_NULL(BF_UnpinBlock(block));
                    CALL_BF_NULL(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                }
                break;
            case LESS_THAN:  /* Looking for less than the value */
                if (*(float *)leaf_entries[entry_pos].key < *(float *)value) {  /* If the key of the next entry that we found is less than the value */
                    break;  /* Done */
                }
                else {  /* If it is greater or equal */
                    entry_pos = INVALID;  /* A smaller key is not going to be found (they are sorted) */
                }
                break;
            case GREATER_THAN:  /* Looking for greater than the value */
                break;  /* Since the previous entry had a greater key the same applies to the next one that we found (because they are sorted) */
            case LESS_THAN_OR_EQUAL:  /* Looking for less than or equal to the value */
                if (*(float *)leaf_entries[entry_pos].key <= *(float *)value) {  /* If the key of the next entry that we found is less than or equal to the value */
                    break;  /* Done */
                }
                else {  /* If it is greater */
                    entry_pos = INVALID;  /* A smaller key is not going to be found (they are sorted) */
                }
                break;
            case GREATER_THAN_OR_EQUAL:  /* Looking for greater than or equal to the value */
                break;  /* Since the previous entry had a greater or equal key the same applies to the next one that we found (because they are sorted) */
        }
    }
    
    else if (*key_type == STRING) {  /* The keys are strings */
        switch (op) {
            case EQUAL:  /* Looking for equal to the value */
                if (strcmp((char *)leaf_entries[entry_pos].key, (char *)value) == 0) {  /* If the key of the next entry that we found is equal to the value */
                    break;  /* Done */
                }
                else {  /* If it is greater (cannot be less, because they are sorted) */
                    entry_pos = INVALID;  /* An equal key is not going to be found (they are sorted) */
                }
                break;
            case NOT_EQUAL:  /* Looking for not equal to the value */
                while(1) {
                    for (; entry_pos <= *entry_counter - 1; entry_pos++) {  /* For each following entry of the current block */
                        if (strcmp((char *)leaf_entries[entry_pos].key, (char *)value) != 0) {  /* If its key is not equal to the value */
                            break;  /* Done */
                        }
                    }
                    if (strcmp((char *)leaf_entries[entry_pos].key, (char *)value) != 0) {  /* If its key is not equal to the value */
                        break;  /* Done */
                    }
                    if (*next_leaf_block == NO_LINK) {  /* If a non-equal key wasn't found and there isn't a next leaf block */
                        entry_pos = INVALID;  /* Mark entry_pos as invalid to indicate EOF */
                        break;
                    }
                    block_pos = *next_leaf_block;  /* Go to the next leaf block */
                    entry_pos = 0;  /* Starting from its first entry */
                    CALL_BF_NULL(BF_UnpinBlock(block));
                    CALL_BF_NULL(BF_GetBlock(fileDesc, block_pos, block));
                    status = (bool *)BF_Block_GetData(block);  /* Update */
                    entry_counter = (int *)(status + 1);  /* Update */
                    next_leaf_block = entry_counter + 1;  /* Update */
                    leaf_entries = (Leaf_Entry *)(next_leaf_block + 1);  /* Update */
                }
                break;
            case LESS_THAN:  /* Looking for less than the value */
                if (strcmp((char *)leaf_entries[entry_pos].key, (char *)value) < 0) {  /* If the key of the next entry that we found is less than the value */
                    break;  /* Done */
                }
                else {  /* If it is greater or equal */
                    entry_pos = INVALID;  /* A smaller key is not going to be found (they are sorted) */
                }
                break;
            case GREATER_THAN:  /* Looking for greater than the value */
                break;  /* Since the previous entry had a greater key the same applies to the next one that we found (because they are sorted) */
            case LESS_THAN_OR_EQUAL:  /* Looking for less than or equal to the value */
                if (strcmp((char *)leaf_entries[entry_pos].key, (char *)value) <= 0) {  /* If the key of the next entry that we found is less than or equal to the value */
                    break;  /* Done */
                }
                else {  /* If it is greater */
                    entry_pos = INVALID;  /* A smaller key is not going to be found (they are sorted) */
                }
                break;
            case GREATER_THAN_OR_EQUAL:  /* Looking for greater than or equal to the value */
                break;  /* Since the previous entry had a greater or equal key the same applies to the next one that we found (because they are sorted) */
        }
    }
    
    else {  /* The key type is invalid */
        AM_errno = AME_INVALID_KEY_TYPE;
        CALL_BF_NULL(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
  	CALL_BF_NULL(BF_UnpinBlock(info_block));
  	BF_Block_Destroy(&info_block);
        return NULL;
    }
    
    CALL_BF_NULL(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    CALL_BF_NULL(BF_UnpinBlock(info_block));
    BF_Block_Destroy(&info_block);
    running_scans[scanDesc].block_pos = block_pos;  /* Update the global array's info */
    running_scans[scanDesc].entry_pos = entry_pos;
    return requested_value;  /* Return the requested value */
}


int AM_CloseIndexScan(int scanDesc) {
  if (scanDesc < 0 || scanDesc > MAX_OPEN_FILES - 1 || running_scans[scanDesc].file_desc == EMPTY) {  /* Check if given scanDesc is valid */
        AM_errno = AME_NO_SUCH_SCAN_TO_CLOSE;
        return AM_errno;
    }
    running_scans[scanDesc].file_desc = EMPTY;  /* Mark this slot as empty */
    running_scans[scanDesc].entry_pos = INVALID;
    return AME_OK;
}


void AM_PrintError(char *errString) {
  if (AM_errno == AME_OK)  /* Finish if there wasn't any error */
  	return;
    printf("%s", errString);  /* Print the given string */
    char message[200];
    switch (AM_errno) {
        case AME_EOF:
            strcpy(message, "Surpassed end of the file");
            break;
        case AME_OPEN_FILES_LIMIT_ERROR:
            strcpy(message, "Exceeded the number of open files");
            break;
        case AME_INVALID_FILE_ERROR:
            strcpy(message, "Invalid file");
            break;
        case AME_ACTIVE_ERROR:
            strcpy(message, "Active error");
            break;
        case AME_FILE_ALREADY_EXISTS:
            strcpy(message, "File already exists");
            break;
        case AME_FULL_MEMORY_ERROR:
            strcpy(message, "Memory is full");
            break;
        case AME_INVALID_BLOCK_NUMBER_ERROR:
            strcpy(message, "Invalid block request");
            break;
        case AME_ERROR:
            strcpy(message, "Error");
            break;
        case AME_ATTEMPT_TO_DESTROY_OPEN_FILE:
            strcpy(message, "Attempt to destroy an open file");
            break;
        case AME_ATTEMPT_TO_CLOSE_FILE_UNDER_SCANNING:
            strcpy(message, "Attempt to close file under scanning");
            break;
        case AME_NOT_BPLUS_FILE_FORMAT:
            strcpy(message, "The file format is not bplus");
            break;
        case AME_RUNNING_SCANS_LIMIT_ERROR:
            strcpy(message, "Exceeded the number of running scans");
            break;
        case AME_ATTEMPT_TO_SCAN_NON_OPEN_FILE:
            strcpy(message, "Attempt to scan a non open file");
            break;
        case AME_INVALID_KEY_TYPE:
            strcpy(message, "Key type is invalid");
            break;
        case AME_NO_SUCH_ENTRY_IN_THIS_FILE:
            strcpy(message, "There is not any entry that fits the requested scan");
            break;
        case AME_ATTEMPT_TO_SCAN_EMPTY_FILE:
            strcpy(message, "Attempt to scan an empty file");
            break;
        case AME_ATTEMPT_TO_CLOSE_NON_OPEN_FILE:
            strcpy(message, "Attempt to close a non open file");
            break;
        case AME_LENGTH_AND_TYPE_MISMATCH:
            strcpy(message, "Given length doesn't match the given type");
            break;
        case AME_NO_SUCH_SCAN_TO_CLOSE:
            strcpy(message, "Attempt to close a non existing scan");
            break;
        case AME_NO_SUCH_SCAN_TO_PERFORM:
            strcpy(message, "Attempt to find the next entry of a non existing scan");
            break;
    }
    printf("%s\n", message);  /* Print the corresponding message */
    AM_errno = AME_OK;  /* Reset AM_errno */
}

void AM_Close() {
}

/* Helper function */
int BF_ERROR_CODE_TO_AME (BF_ErrorCode bf_code) { /* Translates the given BF error code to AME */
    switch(bf_code)
    {
        case BF_OK: return AME_OK;
        case BF_OPEN_FILES_LIMIT_ERROR: return AME_OPEN_FILES_LIMIT_ERROR;
        case BF_INVALID_FILE_ERROR: return AME_INVALID_FILE_ERROR;
        case BF_ACTIVE_ERROR: return AME_ACTIVE_ERROR;
        case BF_FILE_ALREADY_EXISTS: return AME_FILE_ALREADY_EXISTS;
        case BF_FULL_MEMORY_ERROR: return AME_FULL_MEMORY_ERROR;
        case BF_INVALID_BLOCK_NUMBER_ERROR: return AME_INVALID_BLOCK_NUMBER_ERROR;
        case BF_ERROR: return AME_ERROR;
    }
}
