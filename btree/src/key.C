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
    // Compare two keys based on their types
    int *key1Int;
    int *key2Int;
    char *key1Str;
    char *key2Str;
    switch (type) {
        // If it's an integer, compare them as integers
        case attrInteger:
            key1Int = (int *) key1;
            key2Int = (int *) key2;
            if (*key1Int < *key2Int)
                return -1;
            else if (*key1Int == *key2Int)
                return 0;
            else
                return 1;
        // Use string compare to compare string keys
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
void make_entry(KeyDataEntry *targetOut,
                AttrType keyTypeIn, const void *keyIn,
                NodeType nodeTypeIn, DataType dataIn,
                int *entryLengthOut) {
    /*
     * targetOut - The pre-allocated storage to write the key & data to
     * keyTypeIn - The type of the key to write to the target
     * keyIn - The actual key to write to the target
     * nodeTypeIn - The type of the data to write to the target
     * dataIn - The actual data to write to the target
     * entryLengthOut - The length of the target after being written to in bytes
     */

    // Cast the dataIn packet as a character array
    char *targetLoc = (char *) targetOut;

    // Get the size of the keyIn
    int keySize = get_key_length(keyIn, keyTypeIn);
    // Make sure it's not too big
    if (keySize > MAX_KEY_SIZE1) {
        cout << "TRIED TO INSERT KEY TOO LARGE!" << endl;
        return;
    }
    // Make sure that the keyIn size is valid, and perform the memcpy to move the keyIn dataIn to the target
    if (keySize != -1)
        memcpy(targetLoc, keyIn, keySize);

    // Get the size of the dataIn
    int dataSize = nodeTypeIn == INDEX ? sizeof (PageId) : nodeTypeIn == LEAF ? sizeof (RID) : 0;
    // If datasize is not 0, and is valid, then copy the dataIn to the target
    if (dataSize != 0)
        memcpy(targetLoc + keySize, &dataIn, dataSize);

    // Calculate the length of the entry
    *entryLengthOut = keySize + dataSize;
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetKeyOut, DataType *targetDataOut,
                  KeyDataEntry *sourceIn, int entryLengthIn, NodeType nodeTypeIn) {
    /*
     * TargetKeyOut must be pre-allocated to hold the key (KeyType)
     * TargetDataOut must be pre-allocated to hold the data (DataType)
     * SourceIn must contain the data to be written to TargetKeyOut and TargetDataOut
     * EntryLengthIn The length of sourceIn in bytes
     * NodeType The type of the data in SourceIn
     */


    // If we want to get the key & data from a blob, use memcpy
    char *sourceLoc = (char *) sourceIn;

    // Get the length of the data
    int dataLength;
    switch (nodeTypeIn) {
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
    int keyLength = entryLengthIn - dataLength;

    // Perform memcpy if the target to copy to is not null
    if (targetKeyOut != NULL)
        memcpy(targetKeyOut, sourceLoc, keyLength);
    if (targetDataOut != NULL)
        memcpy(targetDataOut, sourceLoc + keyLength, dataLength);
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *keyIn, const AttrType keyTypeIn) {
    /*
     * keyIn is the key to get the length of
     * keyTypeIn is the keyType of the key
     */

    // Get the length of a given keyIn using a switch statement
    switch (keyTypeIn) {
        case attrInteger:
            return sizeof(int);
        case attrString:
            return strlen((char *) keyIn) + 1;
        default:
            return -1;
    }
}

/*
 * get_key_data_length: return (key+data) length in given key_type
 */
int get_key_data_length(const void *keyIn, const AttrType keyTypeIn,
                        const NodeType nodeTypeIn) {
    /*
     * keyIn - The key in DataKeyEntry form to get the length of
     * keyTypeIn - The key type of the key found in key
     * nodeTypeIn - The data type of the data in key
     */

    // Get the length of a keyIn & data blob by adding the key length to the size of the data
    int keyLength = get_key_length(keyIn, keyTypeIn);
    switch (nodeTypeIn) {
        case INDEX:
            return keyLength + sizeof(PageId);
        case LEAF:
            return keyLength + sizeof(RID);
        default:
            return -1;
    }
}
