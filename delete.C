#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
// return OK;

    Status status;
    HeapFileScan scan(relation, status);
    if (status != OK) return status;

    // ----------------------------------
    // CASE 1: UNCONDITIONAL DELETE
    // ----------------------------------
    if (attrName.empty()) {
        status = scan.startScan(0, 0, STRING, nullptr, EQ);
        if (status != OK) return status;

        RID rid;
        while (scan.scanNext(rid) == OK) {
            status = scan.deleteRecord();
            if (status != OK) break;
        }

        scan.endScan();
        return OK;
    }

    // ----------------------------------
    // CASE 2: CONDITIONAL DELETE
    // ----------------------------------
    AttrDesc attr;
    status = attrCat->getInfo(relation, attrName, attr);
    if (status != OK) return status;

    int intVal;
    float floatVal;

    if (type == INTEGER) {
        intVal = atoi(attrValue);
        status = scan.startScan(
            attr.attrOffset,
            attr.attrLen,
            INTEGER,
            (char*)&intVal,
            op
        );
    }
    else if (type == FLOAT) {
        floatVal = atof(attrValue);
        status = scan.startScan(
            attr.attrOffset,
            attr.attrLen,
            FLOAT,
            (char*)&floatVal,
            op
        );
    }
    else { // STRING
        status = scan.startScan(
            attr.attrOffset,
            attr.attrLen,
            STRING,
            attrValue,
            op
        );
    }

    if (status != OK) return status;

    RID rid;
    while (scan.scanNext(rid) == OK) {
        status = scan.deleteRecord();
        if (status != OK) break;
    }

    scan.endScan();
    return OK;
}


