#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
// return OK;
    Status status;

    // Get relation schema
    int relAttrCnt;
    AttrDesc *attrs;
    status = attrCat->getRelInfo(relation, relAttrCnt, attrs);
    if (status != OK) return status;

    if (attrCnt != relAttrCnt)
        return INVALIDRECLEN;

    // Compute record length
    int reclen = 0;
    for (int i = 0; i < relAttrCnt; i++)
        reclen += attrs[i].attrLen;

    char *recBuf = new char[reclen];
    memset(recBuf, 0, reclen);

    // Fill record using offsets
    for (int i = 0; i < attrCnt; i++) {
        bool found = false;

        for (int j = 0; j < relAttrCnt; j++) {
            if (strcmp(attrList[i].attrName, attrs[j].attrName) == 0) {

                if (attrs[j].attrType == INTEGER) {
                    int v = atoi((char *)attrList[i].attrValue);
                    memcpy(recBuf + attrs[j].attrOffset,
                           &v,
                           sizeof(int));
                }
                else if (attrs[j].attrType == FLOAT) {
                    float v = atof((char *)attrList[i].attrValue);
                    memcpy(recBuf + attrs[j].attrOffset,
                           &v,
                           sizeof(float));
                }
                else { // STRING
                    memcpy(recBuf + attrs[j].attrOffset,
                           attrList[i].attrValue,
                           attrs[j].attrLen);
                }

                found = true;
                break;
            }
        }

        if (!found)
            return ATTRNOTFOUND;
    }

    // Insert record
    InsertFileScan scan(relation, status);
    if (status != OK) return status;

    Record rec;
    rec.data = recBuf;
    rec.length = reclen;

    RID rid;
    status = scan.insertRecord(rec, rid);

    delete[] recBuf;
    delete[] attrs;
    return status;
}

