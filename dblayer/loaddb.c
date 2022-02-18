#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "codec.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#include "tbl.h"
#include "util.h"

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

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
    Table_Close(tbl);
    err = PF_CloseFile(indexFD);
    checkerr(err);
    return sch;
}

int
main() {
    loadCSV();
}
