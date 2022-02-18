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


/*
Takes a schema, and an array of strings (fields), and uses the functionality
in codec.c to convert strings into compact binary representations
 */
int
encode(Schema *sch, char **fields, byte *record, int spaceLeft) {
    int n = sch->numColumns;
    int totalSpace = spaceLeft;
    for (int i = 0; i < n; i++) {
        if(sch->columns[i]->type == VARCHAR) {
            int res = EncodeCString(fields[i], record + (totalSpace - spaceLeft), spaceLeft);
            char decoded[1000];
            DecodeCString(record + (totalSpace - spaceLeft), decoded, 10000);
            spaceLeft = spaceLeft - res;
        } else if(sch->columns[i]->type == INT) {
            int res = EncodeInt(atoi(fields[i]), record + (totalSpace - spaceLeft));
            spaceLeft = spaceLeft - res;
        } else if(sch->columns[i]->type == LONG) {
            int res = EncodeLong(atol(fields[i]), record + (totalSpace - spaceLeft));
            spaceLeft = spaceLeft - res;
        }
    }
    return totalSpace - spaceLeft;
    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : EncodeCString
    //        INT : EncodeInt
    //        LONG: EncodeLong
    // return the total number of bytes encoded into record
}

Schema *
loadCSV() {
    // Open csv file, parse schema
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        exit(EXIT_FAILURE);
    }
    
    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
	    fprintf(stderr, "Unable to read data.csv\n");
	    exit(EXIT_FAILURE);
    }
    // Open main db file
    Schema *sch = parseSchema(line);

    Table *tbl;
    if(Table_Open(DB_NAME, sch, true, &tbl) < 0) {
        return NULL;
    }

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    int err;
    if(sch->columns[2]->type == INT) {
        err = AM_CreateIndex(DB_NAME, 0, 'i', 4);
    } else {
        err = -1;
    }
    checkerr(err);
    int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);
    int fd = PF_OpenFile(DB_NAME);
    if(fd < 0) {
        printf("Error occured in opening the file\n");
        return NULL;
    }
    tbl->fd = fd;
    // printf("%s %d %d\n", tbl->dbname, tbl->fd, tbl->schema->numColumns);
    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
        int n = split(line, ",", tokens);
        assert (n == sch->numColumns);
        int len = encode(sch, tokens, record, sizeof(record));
        RecId rid;
        int ret = Table_Insert(tbl, record, len, &rid);
        if(ret < 0) {
            printf("Error Occured in Table insert\n");
            return NULL;
        }
        // printf("%d %s\n", rid, tokens[0]);
        fflush(stdin);
        char record2[MAX_PAGE_SIZE];
        int t = Table_Get(tbl, rid, record2, 10000);
        if ( t < 0) {
            printf("Bad\n");
            return NULL;
        }
        // printf("%d tg\n", t);

        // Indexing on the population column 
        int population = atoi(tokens[2]);

        
        // Use the population field as the field to index on
        err = AM_InsertEntry(indexFD, 'i', 4, (char*)&population, rid);
    
    }
    fclose(fp);
    // Table_Close(tbl);
    err = PF_CloseFile(indexFD);
    checkerr(err);
    return sch;
}


void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema *schema = (Schema *) callbackObj;
    byte *cursor = row;
    int spaceleft = len;
    for(int i = 0; i < schema->numColumns; i++) {
        // printf("\n%d i \n", i);
        if(schema->columns[i]->type == VARCHAR) {
            char decoded[1000];
            // printf("hello\n");
            int l = DecodeCString((byte *)(row+(len-spaceleft)), decoded, 1000);
            // printf("%d: l\n", l);
            if(i==0) {
                printf("%s", decoded);
            }
            else {
                 printf(",%s", decoded);
            }
            spaceleft = spaceleft - l;
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

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
	 
void
index_scan(Table *tbl, Schema *schema, int indexFD, int op, int value) {
    
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
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        exit(EXIT_FAILURE);
    }
    
    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
	    fprintf(stderr, "Unable to read data.csv\n");
	    exit(EXIT_FAILURE);
    }
    // Open main db file
    Schema *sch = parseSchema(line);

    Table *tbl;
    if(Table_Open(DB_NAME, sch, true, &tbl) < 0) {
        return NULL;
    }

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    int err;
    if(sch->columns[2]->type == INT) {
        err = AM_CreateIndex(DB_NAME, 0, 'i', 4);
    } else {
        err = -1;
    }
    checkerr(err);
    int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);
    int fd = PF_OpenFile(DB_NAME);
    if(fd < 0) {
        printf("Error occured in opening the file\n");
        return NULL;
    }
    tbl->fd = fd;
    // printf("%s %d %d\n", tbl->dbname, tbl->fd, tbl->schema->numColumns);
    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
        int n = split(line, ",", tokens);
        assert (n == sch->numColumns);
        int len = encode(sch, tokens, record, sizeof(record));
        RecId rid;
        int ret = Table_Insert(tbl, record, len, &rid);
        if(ret < 0) {
            printf("Error Occured in Table insert\n");
            return NULL;
        }
        printf("%d %s\n", rid, tokens[0]);
        fflush(stdin);
        char record2[MAX_PAGE_SIZE];
        int t = Table_Get(tbl, rid, record2, 10000);
        if ( t < 0) {
            printf("Bad\n");
            return NULL;
        }
        // printf("%d tg\n", t);

        // Indexing on the population column 
        int population = atoi(tokens[2]);

        
        // Use the population field as the field to index on
        err = AM_InsertEntry(indexFD, 'i', 4, (char*)&population, rid);
    
    }
    fclose(fp);
    // Table_Close(tbl);
    err = PF_CloseFile(indexFD);
    checkerr(err);

    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema *schema = parseSchema(schemaTxt);
    // Table *tbl;
    // Table_Open(DB_NAME, schema, false, &tbl);
    // int fd = PF_OpenFile(DB_NAME);
    // if(fd < 0 ) {
    //     printf("error occured\n");
    //     return 1;
    // }
    // tbl->fd = fd;
    if (argc == 2 && *(argv[1]) == 's') {
        Table_Scan(tbl, schema, printRow);
    } else {
	// index scan by default
	int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);

	// Ask for populations less than 100000, then more than 100000. Together they should
	// yield the complete database.
	index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 100000);
	index_scan(tbl, schema, indexFD, GREATER_THAN, 100000);
    }
    Table_Close(tbl);
}
