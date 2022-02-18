#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define min(X, Y) (((X) < (Y)) ? (X) : (Y))
#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

int  getLen(int slot, char *pageBuf) {
    int pointer_to_free_space_value = *(int *)(&pageBuf[5]);
    int num_records = *(int *)(&pageBuf[1]);
    int record_pointer_value = *(int *)(&pageBuf[9+slot]);
    if(slot == num_records - 1) {
        return record_pointer_value - pointer_to_free_space_value;
    }
    else {
        int next_record_pointer_value  = *(int*)(&pageBuf[10+slot]);
        return record_pointer_value - next_record_pointer_value;
    }
    return -1;
}
int  getNumSlots(char *pageBuf) {
    int num_records = *(int *)(&pageBuf[1]);
    return num_records;
}
void setNumSlots(byte *pageBuf, int nslots) {
    *(int *)(&pageBuf[1]) = nslots;
}
int  getNthSlotOffset(int slot, char* pageBuf) {
    int num_records = *(int *)(&pageBuf[1]);
    if(slot >= num_records) return -1;
    int l = getLen(slot, pageBuf);
    int slot_end = *(int*)(&pageBuf[9+slot]);
    return slot_end - l + 1;
}


/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    // Initialize PF, create PF file,
    // allocate Table structure  and initialize and return via ptable
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage.

    PF_Init();
    int res = PF_CreateFile(dbname);

    if (res == PFE_OK || (overwrite && res != PFE_OK))
    {

        if (res != PFE_OK && overwrite)
        {
            res = PF_DestroyFile(dbname); // what else necessaty for destroying file.. whould close page.. buffer.. etc.
            if(res != PFE_OK) {
                printf("Something bad happened. Returning...\n");
                return 1;
            }
            int res = PF_CreateFile(dbname);
            if(res != PFE_OK) {
                printf("The code is buggy. Returning ...\n");
                return 1;
            }
        }

        *ptable = (Table *)malloc(sizeof(Table));
        (*ptable)->schema = (Schema *)malloc(sizeof(Schema));
        (*ptable)->dbname = strdup(dbname);
        (*ptable)->fd = -1;
        (*ptable)->schema->numColumns = schema->numColumns;
        (*ptable)->schema->columns = (ColumnDesc **)malloc(schema->numColumns * sizeof(ColumnDesc *));

        for (int i = 0; i < schema->numColumns; i++)
        {
            (*ptable)->schema->columns[i] = (ColumnDesc *)malloc(sizeof(ColumnDesc));
            (*ptable)->schema->columns[i]->type = schema->columns[i]->type;
            (*ptable)->schema->columns[i]->name = strdup(schema->columns[i]->name);
        }
    } else {
        return 1;
    }

    return 0;
}

void
Table_Close(Table *tbl) {
    // Unfix any dirty pages, close file.
    
    int fd = tbl->fd;

    if(fd<0) {
        printf("The table was not open\n");
        return;
    }
    int *pagenum = (int*)malloc(sizeof(int));
    *pagenum = -1;
    char *pagebuf;
    while(PF_GetNextPage(fd,pagenum, &pagebuf) != PFE_EOF) {
        bool dirty = *(bool*)(&pagebuf[1]);
        if(dirty == true) {
            *(bool*)(&pagebuf[1]) = false;
            PF_UnfixPage(fd, *pagenum - 1,true);
        }
        else {
            PF_UnfixPage(fd, *pagenum - 1,false);
        }
    }
    PF_CloseFile(fd);
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    int fd = tbl->fd;
    if(fd < 0) {
        printf("The table is not yet open\n");
        return 1;
    }
    int *pagenum = (int*)malloc(sizeof(int));
    *pagenum = -1;
    char *pagebuf;
    int found = 0;
    while(PF_GetNextPage(fd,pagenum,&pagebuf) != PFE_EOF) {
        // checking if the page has empty space for the record
        int pointer_to_free_space_value = *(int *)(&pagebuf[5]);
        int num_records = *(int *)(&pagebuf[1]);
        if(pointer_to_free_space_value - (9 + 4*(num_records+1)) >= len) {
            // can be inserted in this page
            found = 1;
            break;
        }
    }

    if(*pagenum == -1 || found == 0) {
        int ret = PF_AllocPage(fd, pagenum, &pagebuf);
        if(ret != PFE_OK) {
            printf("Error occured in insert. Returning...\n");
            return -1;
        }
        *(bool*)(&pagebuf[0]) = true;
        *(int *)(&pagebuf[1]) = 0;
        *(int *)(&pagebuf[5]) = PF_PAGE_SIZE - 1;
    }
    // insert the record
    int pointer_to_free_space_value = *(int *)(&pagebuf[5]);
    int num_records = *(int *)(&pagebuf[1]);
    *(int *)(&pagebuf[1]) = num_records + 1;
    *(int *)(&pagebuf[9+4*num_records]) = pointer_to_free_space_value;
    for(int i = 0 ; i < len; i++) {
        *(byte *)(&pagebuf[pointer_to_free_space_value - len + 1 + i]) = record[i];
    }
    *(int *)(&pagebuf[5]) = pointer_to_free_space_value - len;
    *(bool *)(&pagebuf[0]) = true;
    *rid = num_records + ((*pagenum)<<16);
    free(pagenum);
    return 0;
}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;
    int fd = tbl->fd;
    if(fd < 0) {
        printf("The file is not open get operation on table. Returning...\n");
        return -1;
    }
    char *pagebuf;
    int ret = PF_GetThisPage(fd, pageNum,	&pagebuf);
    if(ret!=PFE_OK) {
        printf("The page corresponding to rid doesn't exist. Returning...\n");
        return -1;
    }
    int offset = getNthSlotOffset(slot, pagebuf);
    if(offset == -1) {
        printf("The slot doesn't exist in the page. Returning..\n");
        return -1;
    }
    int length = getLen(slot, pagebuf);
    for(int i = 0; i < min(maxlen, length); i ++) {
        record[i] = (byte)pagebuf[i];
    }
    return min(maxlen, length);
    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {
    int fd = tbl->fd;
    if(fd < 0) {
        printf("The file is not open get operation on table. Returning...\n");
        return;
    }
    int *pagenum = (int*)malloc(sizeof(int));
    *pagenum = -1;
    char *pagebuf;
    while(PF_GetNextPage(fd,pagenum,&pagebuf) != PFE_EOF) {
        int num_records = *(int *)(&pagebuf[1]);
        for(int i = 0; i < num_records; i++) {
            int rid = i + ((*pagenum)<<16);
            byte record[10000];
            int len = Table_Get(tbl, rid, record, 10000);
            callbackfn(callbackObj, rid, record, len);
        }
    }

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
}


