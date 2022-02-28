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

/* The structure of page is as follows
0th byte -- dirty byte
byte 1-4th -- number of records
byte 5-8th -- pointer to free location
byte 8 onwards -- slots of size 4 bytes and there are num_records number of them
the bytes from last store the records. The slot stores the end index of the record.
And the free pointer stores the index of location - 1 where the records ended.
*/

// takes into consideration the offset of the record and the next record to calculate the length of the record(the difference of the two).
// If the record is the last record then takes the help of free pointer instead of next record
int  getLen(int slot, char *pageBuf) {
    int pointer_to_free_space_value = *(int *)(&pageBuf[5]);
    int num_records = *(int *)(&pageBuf[1]);
    int record_pointer_value = *(int *)(&pageBuf[9+4*slot]);
    if(slot == num_records - 1) {
        return record_pointer_value - pointer_to_free_space_value;
    }
    else {
        int next_record_pointer_value  = *(int*)(&pageBuf[9+4*(slot+1)]);
        return record_pointer_value - next_record_pointer_value;
    }
    return -1;
}

// Equal to number of records
int  getNumSlots(char *pageBuf) {
    int num_records = *(int *)(&pageBuf[1]);
    return num_records;
}

// Setting number of records
void setNumSlots(byte *pageBuf, int nslots) {
    *(int *)(&pageBuf[1]) = nslots;
}

// Since my slot points to the end of the record so take the length and end of the record to caculature the offset of the record.
int  getNthSlotOffset(int slot, char* pageBuf) {
    int num_records = *(int *)(&pageBuf[1]);
    if(slot >= num_records) return -1;
    int l = getLen(slot, pageBuf);
    int slot_end = *(int*)(&pageBuf[9+4*slot]);
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
    int fd= PF_OpenFile(dbname);
    if(fd<0){
        // File doesn't exist so create it and open it
         int err = PF_CreateFile(dbname);
         checkerr(err);
         fd= PF_OpenFile(dbname);
         checkerr(fd);
    }
    else
    {   // file exists
        if(overwrite)
        {   
            // overwrite it if the overwrite flag is set
            checkerr(PF_DestroyFile(dbname));
            int err = PF_CreateFile(dbname);
            checkerr(err);
            fd= PF_OpenFile(dbname);
            checkerr(fd);
    }}

    /// Allocate memory in the table structure
    *ptable = (Table *)malloc(sizeof(Table));
    (*ptable)->fd = fd;
    (*ptable)->schema = (Schema *)malloc(sizeof(Schema));
    (*ptable)->dbname = strdup(dbname);
    (*ptable)->pages = 0;
    int *pagenum = (int*)malloc(sizeof(int));
    *pagenum = -1;
    char *pagebuf;

    // setting num pages
    while(PF_GetNextPage(fd,pagenum, &pagebuf) != PFE_EOF) {
        (*ptable)->pages++;
        int ret =PF_UnfixPage(fd, *pagenum, false);
        if (ret < 0) {
            printf("This is bad1.\n");
        }
    }
    // copying schema into table struct
    (*ptable)->schema->numColumns = schema->numColumns;
    (*ptable)->schema->columns = (ColumnDesc **)malloc(schema->numColumns * sizeof(ColumnDesc *));

    for (int i = 0; i < schema->numColumns; i++)
    {
        (*ptable)->schema->columns[i] = (ColumnDesc *)malloc(sizeof(ColumnDesc));
        (*ptable)->schema->columns[i]->type = schema->columns[i]->type;
        (*ptable)->schema->columns[i]->name = strdup(schema->columns[i]->name);
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

    // iterate over pages and if they are dirty then unfix them. Dirty byte is the first byte of the page. 
    // In our implmentation the byte would never be dirty as the page is always unfixed after insert operation
    int *pagenum = (int*)malloc(sizeof(int));
    *pagenum = -1;
    char *pagebuf;
    while(PF_GetNextPage(fd,pagenum, &pagebuf) != PFE_EOF) {
        bool dirty = *(bool*)(&pagebuf[0]);
        if(dirty == true) {
            *(bool*)(&pagebuf[0]) = false;
            PF_UnfixPage(fd, *pagenum,true);
        }
        else {
            PF_UnfixPage(fd, *pagenum,false);
        }
    }
    // close the file that would write the header also in the file
    int ret = PF_CloseFile(fd);
    if (ret < 0) {
        printf("Unable to close file!\n");
    }
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
    *pagenum = tbl->pages-1;
    char *pagebuf;
    int found = 0;
    // Get the last page of the table
    if(PF_GetThisPage(fd,*pagenum,&pagebuf) == PFE_OK) {
        // checking if the page has empty space for the record
        int pointer_to_free_space_value = *(int *)(&pagebuf[5]);
        int num_records = *(int *)(&pagebuf[1]);
        if(pointer_to_free_space_value - (9 + 4*(num_records+1)) >= len) {
            // can be inserted in this page
            found = 1;
        }
        else {
            // Need to allocate a new page to insert the record, unfix the current page
            int ret =PF_UnfixPage(fd, *pagenum, false);
            if (ret < 0) {
                printf("This is bad1.\n");
            }
        }
    }
    if(*pagenum == -1 || found == 0) {
        // Allocating new page
        int ret = PF_AllocPage(fd, pagenum, &pagebuf);
        if(ret != PFE_OK) {
            printf("Error occured in insert. Returning...\n");
            return -1;
        }
        tbl->pages++;
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
    *(bool *)(&pagebuf[0]) = false;
    *rid = num_records + ((*pagenum)<<16);

    // unfix the page after inserting
    int ret = PF_UnfixPage(fd, *pagenum, true);
    if (ret < 0) {
        printf("This is bad.\n");
    }

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
    // slot and pagenum
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;
    int fd = tbl->fd;
    if(fd < 0) {
        printf("The file is not open, get operation on table. Returning...\n");
        return -1;
    }
    char *pagebuf;
    // Get the page
    int ret = PF_GetThisPage(fd, pageNum,	&pagebuf);
    if(ret!=PFE_OK) {
        printf("The page corresponding to rid doesn't exist. Returning...\n");
        return -1;
    }
    int offset = getNthSlotOffset(slot, pagebuf);

    if(offset < 0) {
        printf("The slot doesn't exist in the page. Returning..\n");
        return -1;
    }
    // using offset and length get the record
    int length = getLen(slot, pagebuf);
    memcpy(record, pagebuf + offset,min(maxlen, length));
    // unfix the page
    PF_UnfixPage(fd, pageNum, false);
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
    // iterate over all pages of the table
    int *pagenum = (int*)malloc(sizeof(int));
    *pagenum = -1;
    char *pagebuf;
    while(PF_GetNextPage(fd,pagenum,&pagebuf) != PFE_EOF) {
        int num_records = *(int *)(&pagebuf[1]);
        int pointer_to_free_space_value = *(int *)(&pagebuf[5]);
        PF_UnfixPage(fd, *pagenum, false);
        // iterate over all records in the page
        for(int i = 0; i < num_records; i++) {
            int rid = i + ((*pagenum)<<16);
            byte record[10000];
            int len = Table_Get(tbl, rid, record, 10000);
            // print the row using the call back function
            callbackfn(callbackObj, rid, record, len);
        }
    }

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
}


