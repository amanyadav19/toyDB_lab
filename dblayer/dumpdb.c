#include <stdio.h>
#include <stdlib.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "codec.h"


#define MAX_PAGE_SIZE 4000


#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define CSV_NAME "data.csv"


void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema *schema = (Schema *) callbackObj;
    byte *cursor = row;
    int spaceleft = len;
    for(int i = 0; i < schema->numColumns; i++) {
        if(schema->columns[i]->type == VARCHAR) {
            char decoded[1000];
            int l = DecodeCString((byte *)(row+(len-spaceleft)), decoded, 1000);
            if(i==0) {
                printf("%s", decoded);
            }
            else {
                 printf(",%s", decoded);
            }
            spaceleft = spaceleft - (l+2);
        } else if(schema->columns[i]->type == INT) {
            int decoded = DecodeInt((byte *)(row+(len-spaceleft)));
            if(i==0) {
                printf("%d", decoded);
            }
            else {
                 printf(",%d", decoded);
            }
            spaceleft = spaceleft - 4;
        } else if(schema->columns[i]->type == LONG) {
            long long decoded = DecodeLong((byte *)(row + len-spaceleft));
            if(i==0) {
                printf("%lld", decoded);
            }
            else {
                 printf(",%lld", decoded);
            }
            spaceleft = spaceleft - 8;
        }
    }
    printf("\n");
}
	 
void
index_scan(Table *tbl, Schema *schema, int indexFD, int op, int value) {
    int scanDesc = AM_OpenIndexScan(indexFD, 'i',4, op, (char*)&value );
    while(true) {
        int rid = AM_FindNextEntry(scanDesc);
        if(rid == AME_EOF) {
            break;
        }
        char record[MAX_PAGE_SIZE];
        int t = Table_Get(tbl, rid, record, 10000);
        if(t < 0) {
            printf("Error in index scan. Exiting...\n");
        }
        printRow(schema, rid, record, t);
    }
    AM_CloseIndexScan(scanDesc);
    /*
    Open index ...
    while (true) {
	find next entry in index
	fetch rid from table
        printRow(...)
    }
    close index ...
    */
}

int
main(int argc, char **argv) {
    // Open csv file, parse schema
    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema *schema = parseSchema(schemaTxt);
    Table *tbl;
    Table_Open(DB_NAME, schema, false, &tbl);
    if (argc == 2 && *(argv[1]) == 's') {
        Table_Scan(tbl, schema, printRow);
    } else {
	// index scan by default
	int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);

	// Ask for populations less than 100000, then more than 100000. Together they should
	// yield the complete database.
	// index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 1000000);
    int val;
    if(strcmp(argv[2], "GREATER_THAN") == 0) {
        val = GREATER_THAN;
    }
    else if(strcmp(argv[2], "LESS_THAN_EQUAL") == 0) {
        val = LESS_THAN_EQUAL;
    }
    else if(strcmp(argv[2], "LESS_THAN") == 0) {
        val = LESS_THAN;
    }
    else if(strcmp(argv[2], "GREATER_THAN_EQUAL") == 0) {
        val = GREATER_THAN_EQUAL;
    }
    else if(strcmp(argv[2], "EQUAL") == 0) {
        val = EQUAL;
    }
	index_scan(tbl, schema, indexFD, val, atoi(argv[3]));
    }
    Table_Close(tbl);
}
