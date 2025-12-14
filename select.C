#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"
#include <cstring>   // for memcpy
#include <iostream>


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

 /*
 Selects records from a relation based on a selection condition and
projects specified attributes into a result relation.
    * Parameters:
        - result: Name of the relation to store the result.
        - projCnt: Number of attributes to project.
        - projNames: Array of attrInfo structures specifying attributes to project.
        - attr: Pointer to attrInfo structure specifying selection attribute (nullptr for no selection).
        - op: Operator for selection condition (e.g., EQ, LT).
        - attrValue: Value to compare against for selection.
    Returns:
        - OK on success.
        - Appropriate error code on failure.
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
    AttrDesc *projDesc = new AttrDesc[projCnt]; // to hold converted projection list
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

    // If attr is not null, get its AttrDesc
    if (attr != nullptr) {
        status = attrCat->getInfo(attr->relName, attr->attrName, selDesc);
        if (status != OK) return status;
        selPtr = &selDesc;
    }

    // Call ScanSelect to perform the selection and projection
    status = ScanSelect(
        result,
        projCnt,
        projDesc,
        selPtr,
        op,
        attrValue,
        reclen
    );

    // Clean up
    delete[] projDesc;
    return status;

}

/*
 * Performs selection and projection using a heap file scan.
 * 
 * Detailed Description:
 * Scan selects records from a relation based on a selection condition and 
 * projects specified attributes into a result relation.
 * It checks the attribute type to set up the scan correctly to select between INTEGER, FLOAT, and STRING types.
 * It uses HeapFileScan to iterate through the records, applies the selection condition,
 * and constructs the projected records to insert into the result relation.
 * It handles both conditional and unconditional scans based on whether attrDesc is provided.
 * After processing all records, it cleans up and returns the appropriate status.
 * 
 *
 * Parameters:
 * 	result: Name of the relation to store the result.
 * 	projCnt: Number of attributes to project.
 * 	projNames: Array of AttrDesc structures specifying attributes to project.
 * 	attrDesc: Pointer to AttrDesc structure specifying selection attribute (nullptr for no selection).
 * 	op: Operator for selection condition (e.g., EQ, LT).
 * 	filter: Value to compare against for selection.
 * 	reclen: Length of the projected record.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */


const Status ScanSelect(const string & result, 
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

    // Scan through records and project attributes
    RID rid;
    Record rec;
    char *outRec = new char[reclen];

    Status s;
    while ((s = scan.scanNext(rid)) == OK) {
        status = scan.getRecord(rec);
        if (status != OK)
        {
            delete[] outRec;
            scan.endScan();
            return status;
        } 
        
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
        if (status != OK) 
        {
            delete[] outRec;
            scan.endScan();
            return status;
        }
    }

    
    delete[] outRec;
    scan.endScan();

    if(s != FILEEOF)
        return s;

    return OK;
}
