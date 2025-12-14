#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
    // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

    Status status;

    // Convert projection list to AttrDesc list
    AttrDesc *projDesc = new AttrDesc[projCnt];
    int reclen = 0;

    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(
            projNames[i].relName,
            projNames[i].attrName,
            projDesc[i]
        );
        if (status != OK) return status;
        reclen += projDesc[i].attrLen;
    }

    // Convert selection attribute (if any)
    AttrDesc selDesc;
    AttrDesc *selPtr = nullptr;

    if (attr != nullptr) {
        status = attrCat->getInfo(attr->relName, attr->attrName, selDesc);
        if (status != OK) return status;
        selPtr = &selDesc;
    }

    status = ScanSelect(
        result,
        projCnt,
        projDesc,
        selPtr,
        op,
        attrValue,
        reclen
    );

    delete[] projDesc;
    return status;

}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;

    // Open result table
    InsertFileScan resultScan(result, status);
    if (status != OK) return status;

    // Input relation name (from projection list)
    string inRel = projNames[0].relName;
    HeapFileScan scan(inRel, status);
    if (status != OK) return status;

    // -----------------------------
    // START SCAN (FIXED PART)
    // -----------------------------
    int intVal;
    float floatVal;

    if (attrDesc == nullptr) {
        // Unconditional scan
        status = scan.startScan(0, 0, STRING, nullptr, EQ);
    } else {
        if (attrDesc->attrType == INTEGER) {
            intVal = atoi(filter);
            status = scan.startScan(
                attrDesc->attrOffset,
                attrDesc->attrLen,
                INTEGER,
                (char*)&intVal,
                op
            );
        }
        else if (attrDesc->attrType == FLOAT) {
            floatVal = atof(filter);
            status = scan.startScan(
                attrDesc->attrOffset,
                attrDesc->attrLen,
                FLOAT,
                (char*)&floatVal,
                op
            );
        }
        else { // STRING
            status = scan.startScan(
                attrDesc->attrOffset,
                attrDesc->attrLen,
                STRING,
                filter,
                op
            );
        }
    }

    if (status != OK) return status;

    // -----------------------------
    // SCAN + PROJECTION
    // -----------------------------
    RID rid;
    Record rec;
    char *outRec = new char[reclen];

    while (scan.scanNext(rid) == OK) {
        status = scan.getRecord(rec);
        if (status != OK) break;

        int offset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(
                outRec + offset,
                (char*)rec.data + projNames[i].attrOffset,
                projNames[i].attrLen
            );
            offset += projNames[i].attrLen;
        }

        Record out;
        out.data = outRec;
        out.length = reclen;

        RID outRid;
        status = resultScan.insertRecord(out, outRid);
        if (status != OK) break;
    }

    scan.endScan();
    delete[] outRec;
    return OK;
}
