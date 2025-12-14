#include "catalog.h"
#include "query.h"
#include <cstring>
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

    // 1. Look up attribute information from AttrCat
    AttrDesc attrDesc;
    status = attrCat->getInfo(relation, attrName, attrDesc);
    if (status != OK) {
        return status;
    }

    // 2. Convert attrValue to correct type
    int intVal;
    float floatVal;
    void *value;

    switch (type) {
        case INTEGER:
            intVal = atoi(attrValue);
            value = &intVal;
            break;

        case FLOAT:
            floatVal = atof(attrValue);
            value = &floatVal;
            break;

        case STRING:
            value = (void *)attrValue;
            break;

        default:
            return BADTYPE;
    }

    // 3. Open a heap file scan with a filter
    HeapFileScan scan(relation, status);
    if (status != OK) {
        return status;
    }

    status = scan.startScan(
        attrDesc.attrOffset,
        attrDesc.attrLen,
        attrDesc.attrType,
        value,
        op
    );
    if (status != OK) {
        return status;
    }

    // 4. Scan and delete matching records
    RID rid;
    while ((status = scan.getNext(rid)) == OK) {
        status = scan.deleteRecord();
        if (status != OK) {
            scan.endScan();
            return status;
        }
    }

    // 5. Finish scan
    scan.endScan();

    // getNext returns FILEEOF when done — that is success
    if (status == FILEEOF) {
        return OK;
    }

    return status;
}
