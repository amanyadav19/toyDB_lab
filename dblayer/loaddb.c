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
    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : EncodeCString
    //        INT : EncodeInt
    //        LONG: EncodeLong
    // return the total number of bytes encoded into record
    int n = sch->numColumns;
    int totalSpace = spaceLeft;
    for (int i = 0; i < n; i++) {
        if(sch->columns[i]->type == VARCHAR) {
            int res = EncodeCString(fields[i], record + (totalSpace - spaceLeft), spaceLeft);
            spaceLeft = spaceLeft - res; // counting how much space is left
        } else if(sch->columns[i]->type == INT) {
            int res = EncodeInt(atoi(fields[i]), record + (totalSpace - spaceLeft));
            spaceLeft = spaceLeft - res;
        } else if(sch->columns[i]->type == LONG) {
            int res = EncodeLong(atol(fields[i]), record + (totalSpace - spaceLeft));
            spaceLeft = spaceLeft - res;
        }
    }
    return totalSpace - spaceLeft;
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
    // Opening the table
    if(Table_Open(DB_NAME, sch, false, &tbl) < 0) {
        return NULL;
    }
    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    int err;
    // Creating index and opening the file
    if(sch->columns[2]->type == INT) {
        err = AM_CreateIndex(DB_NAME, 0, 'i', 4);
    } else {
        err = -1;
    }
    checkerr(err);
    int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);
    // iterate over all lines in the file
    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
        int n = split(line, ",", tokens);
        assert (n == sch->numColumns);
        // encode the record and insert into table
        int len = encode(sch, tokens, record, sizeof(record));
        RecId rid;
        int ret = Table_Insert(tbl, record, len, &rid);
        if(ret < 0) {
            printf("Error Occured in Table insert\n");
            return NULL;
        }
        printf("%d %s\n", rid, tokens[0]);
        fflush(stdin);
        // Indexing on the population column 
        int population = atoi(tokens[2]);

        // Use the population field as the field to index on
        err = AM_InsertEntry(indexFD, 'i', 4, (char*)&population, rid);
    
    }
    // close the csv file, table, and index file
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
