/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>

#include "../include/bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType type) {
    int *key1Int;
    int *key2Int;
    char *key1Str;
    char *key2Str;
    switch (type) {
        case attrInteger:
            key1Int = (int *) key1;
            key2Int = (int *) key2;
            if (*key1Int < *key2Int)
                return -1;
            else if (*key1Int == *key2Int)
                return 0;
            else
                return 1;
        case attrString:
            key1Str = (char *) key1;
            key2Str = (char *) key2;
            return strcmp(key1Str, key2Str);
        default:
            return 0;
    }
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType keyType, const void *key,
                NodeType nodeType, DataType data,
                int *entryLength) {
    char *targetLoc = (char *) target;

    int keySize = get_key_length(key, keyType);
    if (keySize > MAX_KEY_SIZE1) {
        cout << "TRIED TO INSERT KEY TOO LARGE!" << endl;
        return;
    }
    if (keySize != -1)
        memcpy(targetLoc, key, keySize);

    int dataSize = nodeType == INDEX ? sizeof (PageId) : nodeType == LEAF ? sizeof (RID) : 0;
    if (dataSize != 0)
        memcpy(targetLoc + keySize, &data, dataSize);

    *entryLength = keySize + dataSize;
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetKey, DataType *targetData,
                  KeyDataEntry *source, int entryLength, NodeType nodeType) {
    char *sourceLoc = (char *) source;

    int dataLength;
    switch (nodeType) {
        case INDEX:
            dataLength = sizeof(PageId);
            break;
        case LEAF:
            dataLength = sizeof(RID);
            break;
        default:
            return;
    }
    // We can calculate the length of the key using the length of the entire entry
    int keyLength = entryLength - dataLength;

    if (targetKey != NULL)
        memcpy(targetKey, sourceLoc, keyLength);
    if (targetData != NULL)
        memcpy(targetData, sourceLoc + keyLength, dataLength);
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType keyType) {
    switch (keyType) {
        case attrInteger:
            return sizeof(int);
        case attrString:
            return strlen((char *) key) + 1;
        default:
            return -1;
    }
}

/*
 * get_key_data_length: return (key+data) length in given key_type
 */
int get_key_data_length(const void *key, const AttrType keyType,
                        const NodeType nodeType) {
    int keyLength = get_key_length(key, keyType);
    switch (nodeType) {
        case INDEX:
            return keyLength + sizeof(PageId);
        case LEAF:
            return keyLength + sizeof(RID);
        default:
            return -1;
    }
}
