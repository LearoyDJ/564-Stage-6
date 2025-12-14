#include "catalog.h"
#include "query.h"
#include <cstdlib>

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

const Status QU_Delete(const string & relation,
                       const string & attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue)
{
    Status status;

    // Case 1: DELETE FROM relation;  (no WHERE clause)
    if (attrName.empty()) {
        HeapFileScan scan(relation, status);
        if (status != OK) return status;

        RID rid;
        while ((status = scan.scanNext(rid)) == OK) {
            status = scan.deleteRecord();
            if (status != OK) {
                scan.endScan();
                return status;
            }
        }
        scan.endScan();
        return (status == FILEEOF) ? OK : status;
    }

    // Look up attribute info
    AttrDesc attrDesc;
    status = attrCat->getInfo(relation, attrName, attrDesc);
    if (status != OK) return status;

    // Convert attribute value
    int intVal;
    float floatVal;
    const char *filter;

    switch (type) {
        case INTEGER:
            intVal = atoi(attrValue);
            filter = (char*)&intVal;
            break;

        case FLOAT:
            floatVal = atof(attrValue);
            filter = (char*)&floatVal;
            break;

        case STRING:
            filter = attrValue;
            break;

        default:
            return OK;   // Minirel usually just ignores bad types
    }

    // Open filtered scan
    HeapFileScan scan(relation, status);
    if (status != OK) return status;

    status = scan.startScan(
        attrDesc.attrOffset,
        attrDesc.attrLen,
        (Datatype) attrDesc.attrType,
        filter,
        op
    );
    if (status != OK) return status;

    // Delete matching records
    RID rid;
    while ((status = scan.scanNext(rid)) == OK) {
        status = scan.deleteRecord();
        if (status != OK) {
            scan.endScan();
            return status;
        }
    }

    scan.endScan();
    return (status == FILEEOF) ? OK : status;
}
