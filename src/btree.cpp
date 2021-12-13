/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

/**
 * BTreeIndex Constructor.
 * Check to see if the corresponding index file exists. If so, open the file.
 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
 *
 * @param relationName        Name of file.
 * @param outIndexName        Return the name of index file.
 * @param bufMgrIn			  Buffer Manager Instance
 * @param attrByteOffset	  Offset of attribute, over which index is to be built, in the record
 * @param attrType			  Datatype of attribute over which index is built
 * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
 */

BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName,
                       BufMgr *bufMgrIn,
                       const int attrByteOffset,
                       const Datatype attrType) {
    // initialize variables
    this->attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
    scanExecuting = false;
    bufMgr = bufMgrIn;
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    // Want to begin by inserting into the root
    insertInRoot = true;
    Page *metaPage;
    Page *rootPage;
    // check correct datatype
    if (attrType != INTEGER) {
        std::cout << "Incorrect data type.";
        return;
    }
    // Creating the index file name, taken from the project specification
    std ::ostringstream indexStr;
    indexStr << relationName << '.' << attrByteOffset;
    outIndexName = indexStr.str();
    // Since meta contains information about the first page, we have to get the header page
    // but also have to see if the file exists.
    try {
        file = new BlobFile(outIndexName, false);
        headerPageNum = file->getFirstPageNo();

        // After we get the first page, we use meta's info to compare with the given info to see if it matches.
        bufMgr->readPage(file, headerPageNum, metaPage);
        IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
        if (relationName != meta->relationName) throw BadIndexInfoException("Index doesn't exist.");
        if (attributeType != meta->attrType) throw BadIndexInfoException("Index doesn't exist.");
        if (attrByteOffset != meta->attrByteOffset) throw BadIndexInfoException("Index  doesn't exist.");

        // unpin headerPage because we are finished with it but didn't modify it.
        bufMgr->unPinPage(file, headerPageNum, false);

    } catch (FileNotFoundException) {  // This means file doesn't exist so create a file.
        file = new BlobFile(outIndexName, true);

        // create a meta page for the new index and fill out it's information.
        bufMgr->allocPage(file, headerPageNum, metaPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);
        IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
        strncpy(meta->relationName, relationName.c_str(), 20);
        meta->attrType = attrType;
        meta->rootPageNo = rootPageNum;

        // Create the root.
        bufMgr->allocPage(file, rootPageNum, rootPage);
        NonLeafNodeInt *root = (NonLeafNodeInt *)rootPage;

        FileScan FS(relationName, bufMgr);
        // get every tuple from the relation and load into the new file.
        RecordId rid;
        while (true) {  // get all rids that meet the predicate and break if the end of the file of relationName is reached.
            try {
                // get
                FS.scanNext(rid);
                insertEntry(FS.getRecord().c_str() + attrByteOffset, rid);
            } catch (EndOfFileException e) {
                break;
            }
        }
        // finished with the meta page and root page and we did modify it.
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
    }
}

/**
 * BTreeIndex Destructor.
 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
 * and delete file instance thereby closing the index file.
 * Destructor should not throw any exceptions. All exceptions should be caught in here itself.
 * */
BTreeIndex::~BTreeIndex() {
    // The destructor should end any scan, clear state variables, unpin pinned pages, and flush index file,
    // and deletes the file object.
    if (scanExecuting) {  // If the scan is executing, then end it.
        try {
            endScan();
        } catch (ScanNotInitializedException) {
            std::cout << "No scan has been initialized.";
        }
    }
    // clears state variables and delete file instance.
    scanExecuting = false;
    bufMgr->flushFile(file);
    delete file;
}

/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 * @param key			Key to insert, pointer to integer/double/char string
 * @param rid			Record ID of a record whose entry is getting inserted into the index.
 **/
void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    if (insertInRoot) {
        //! debug
        std::cout << "Insert entry: " << *((int *)key) << " into rootnode" << std::endl;

        // case 1 - there hasn't been a split yet, ie, all nodes inserted into root node (leaf node)
        // call insert directly
        insertIntoNonLeafNode(rootPageNum, key);
    } else {
        //! debug
        std::cout << "Regular insert: " << *((int *)key) << std::endl;

        // case 2 - traverse tree till you find a leaf node to insert into
        PageId resultPage;
        searchNode(resultPage, key, rootPageNum);

        // then call insert on the result
        insertIntoLeafNode(resultPage, rid, key);
    }
}

/**
 * This method is called when inserting a value into a non leaf node. It checks to see if there is space available
 * and if not it calls splitNode
 *
 * @param pid			pageId of the page the value is to be inserted into
 * @param key			Key to insert, pointer to integer/double/char string
 **/
void BTreeIndex::insertIntoNonLeafNode(const PageId pid, const void *key) {
    // declare and read the current page
    Page *curPage;
    bufMgr->readPage(file, pid, curPage);

    // creating and initializing a node
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    // check if there is any space available within this non leaf node
    if (curNode->spaceAvail > 0) {
        // count how many spaces are full within node
        int size = INTARRAYNONLEAFSIZE - curNode->spaceAvail;

        // add key value to the node
        curNode->keyArray[size] = *((int *)key);
        // decrement the availSpace
        curNode->spaceAvail--;
        // for loop to traverse through the keyArray
        // so it is in sorted order
        for (int i = 0; i < size; i++) {
            if (curNode->keyArray[size - i] < curNode->keyArray[size - i - 1])
                // swap key values
                int temp = curNode->keyArray[size - i - 1];
            curNode->keyArray[size - i - 1] = curNode->keyArray[size - i];
            curNode->keyArray[size - i] = size;
        }
        // unpin page, call split non leaf node
        bufMgr->unPinPage(file, pid, true);
    }
    // no space availble in node
    // call splitNonLeafNode
    // also need to check if it is a root Node from within split non leaf node
    else {
        // No room now, need to split and push up
        // bufMgr->unPinPage(file, pid, true);
        // check if we need to create a root node
        // call split non leaf node if full
        // initialize leftChild
        // initialize rightChild
        bufMgr->unPinPage(file, pid, false);
        splitNonLeafNode(pid, key);
    }
}

/**
 * Inserts new entry into a leaf node if the node has space left, if not, splitLeafNode will be called.
 *
 * @param pid           Page ID of leaf node
 * @param rid			Record ID of a record whose entry is getting inserted into the index.
 * @param key			Key to insert, pointer to integer/double/char string
 */
void BTreeIndex::insertIntoLeafNode(const PageId pid, const RecordId rid, const void *key) {
    // declare and read the current page
    Page *curPage;
    bufMgr->readPage(file, pid, curPage);

    // initialize the leaf node what we're insert into
    LeafNodeInt *curNode = (LeafNodeInt *)curPage;
    // check if there is space within the current leafNode
    if (curNode->spaceAvail > 0) {
        // If there's room in this node
        int numNode = INTARRAYLEAFSIZE - curNode->spaceAvail;  // How many pages are in this node

        // First add the new entry to the end of the array
        curNode->ridArray[numNode] = rid;
        curNode->keyArray[numNode] = *((int *)key);

        // Then bubble sort the entries into the right place
        for (int i = numNode; i > 0; i++) {
            if (curNode->keyArray[i] < curNode->keyArray[i - 1]) {
                // Swaps the lower value and the inserted value by saving a temporay variable
                int tempKey = curNode->keyArray[i - 1];
                RecordId tempId = curNode->ridArray[i - 1];

                curNode->keyArray[i - 1] = curNode->keyArray[i];
                curNode->ridArray[i - 1] = curNode->ridArray[i];

                curNode->keyArray[i] = tempKey;
                curNode->ridArray[i] = tempId;
            } else {
                // If it reaches here, that means the new key is in the right spot
                break;
            }
        }
        // use up one aviliable space
        curNode->spaceAvail--;
        bufMgr->unPinPage(file, pid, true);
    } else {
        // No room now, need to split and push up
        bufMgr->unPinPage(file, pid, true);
        splitLeafNode(key, rid, pid);
    }
}

/**
 * This method is called when the top of the tree is reached and we have to create a new root node.
 *
 * @param key			Key to insert, pointer to integer/double/char string
 * @param leftChild			Key to insert, pointer to integer/double/char string
 * @param rightChild			Key to insert, pointer to integer/double/char string
 * @param key			Key to insert, pointer to integer/double/char string
 **/
void BTreeIndex::createNewRoot(const void *key, const PageId leftChild, const PageId rightChild, bool aboveLeaf) {
    // declare rootID
    PageId rootId;
    // declare rootPage
    Page *rootPage;
    // allocate a new page for root node
    bufMgr->allocPage(this->file, rootId, rootPage);
    // itialize new root node
    NonLeafNodeInt *rootNode = (NonLeafNodeInt *)rootPage;
    // update new non leaf node
    if (aboveLeaf) {
        rootNode->level = 1;
    } else {
        rootNode->level = 0;
    }
    rootNode->keyArray[INTARRAYLEAFSIZE - rootNode->spaceAvail] = *((int *)key);
    // add left child
    rootNode->pageNoArray[0] = leftChild;
    // add right child
    rootNode->pageNoArray[1] = rightChild;
    rootNode->spaceAvail--;
    Page *metaPage;
    bufMgr->readPage(file, headerPageNum, metaPage);
    IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
    meta->rootPageNo = rootId;
    // unpin page
    this->bufMgr->unPinPage(this->file, rootId, true);
}

/**
 * Recursive function to traverse the B+ Tree and find the node with the coorsponding key value
 *
 * @param key   the void pointer of the key to be searched
 * @param currentId
 */
void BTreeIndex::searchNode(PageId &pid, const void *key, PageId currentId) {
    // Reads the content of currentId into curPage
    Page *curPage;
    bufMgr->readPage(file, currentId, curPage);

    int keyInt = *((int *)key);  // Key of data, but in int form
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    int numPage = INTARRAYNONLEAFSIZE - curNode->spaceAvail;  // How many pages are in this node
    int numChild = numPage + 1;                               // How many children nodes this node points to

    //! debug
    std::cout << "Searching for : " << keyInt << std::endl;

    //  search throught the key list of the current node until we reach the leaf node
    for (int i = 0; i < numPage; i++) {
        //! debug
        std::cout << "comparing against : " << curNode->keyArray[i] << std::endl;

        PageId targetId;
        bool found = false;

        // Start comparing against the smallest key in this node
        // If it is true, then the pid at the current location is the target page
        // Since we are right biased, it's only until the key is less than current node,
        // otherwise, we'd use <=
        if (keyInt < curNode->keyArray[i]) {
            targetId = curNode->pageNoArray[i];
            found = true;

            // ! debug
            std::cout << "Insert into before: " << curNode->keyArray[i] << std::endl;

        } else if (keyInt > curNode->keyArray[numPage]) {
            // This is the edge case if the key is larger than all keys in this node, then we jump straight to the
            // right most pointer
            targetId = curNode->pageNoArray[numChild - 1];
            found = true;

            // ! debug
            std::cout << "Going to the right most pointer, bigger than: " << curNode->keyArray[numPage] << std::endl;
        }

        if (found) {
            // Found a spot, set the parent of node at the destination as the current node
            Page *targetPage;
            bufMgr->readPage(file, targetId, targetPage);
            LeafNodeInt *targetNode = (LeafNodeInt *)targetPage;

            // Sets the parrent of the leaf node to current node
            targetNode->parentId = currentId;

            // Whether we reached the end or not
            if (curNode->level == 1) {
                bufMgr->unPinPage(file, currentId, false);
                // this means we are right above the target leaf node
                // Save the result
                pid = targetId;
            } else {
                // means we found a spot, but they're not a leaf node, so we must go even furthur
                bufMgr->unPinPage(file, currentId, false);
                searchNode(pid, key, targetId);
            }
        }
    }
}

/**
 * This method is called when a node is at max capacity already so it needs to be split.
 * It is a recursive method that pushes values up the tree as it is split
 * if the given node is full then splitNonLeafNode is called again from within.
 *
 * @param pid   Page ID of the result
 * @param key   the void pointer of the key to be searched
 */
void BTreeIndex::splitNonLeafNode(const PageId pid, const void *key) {
    // declares a page
    Page *curPage;
    // reads the page to split
    bufMgr->readPage(this->file, pid, curPage);
    // itilaize the non leaf node to split
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    // Create the new page(sibling)
    Page *newPage;
    PageId newPageId;
    // allocate page
    this->bufMgr->allocPage(this->file, newPageId, newPage);
    // create node
    NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;
    // assign variables to sibling
    newNode->level = curNode->level;
    newNode->parentId = curNode->parentId;
    // udpdate the new nodes values
    for (int i = 0; i < INTARRAYNONLEAFSIZE / 2; i++) {
        // sets the split node to the second half values of the current node
        newNode->keyArray[i] = curNode->keyArray[INTARRAYNONLEAFSIZE / 2 + i];
        // delete the values from the current Node after adding to split node
        newNode->keyArray[INTARRAYNONLEAFSIZE / 2 + i] = NULL;
        // update the children of the new node
        newNode->pageNoArray[i] = curNode->pageNoArray[INTARRAYNONLEAFSIZE / 2 + i + 1];
    }
    // addedNewNode keeps track of where the new node was added
    // this will be used to find which key to push up
    bool addedNewNode = false;
    // check where to insert value
    // compare to the last value in the curNode and if it is less than or equal then insert into current Node
    if (curNode->keyArray[INTARRAYLEAFSIZE / 2 - 1] >= *((int *)key)) {
        // call insert for the curNode
        insertIntoNonLeafNode(pid, key);
    }
    // else call insert on the newNode created
    else {
        insertIntoNonLeafNode(newPageId, key);
        addedNewNode = true;
    }
    // insert value
    // unpin current page
    bufMgr->unPinPage(file, pid, false);
    // unpin new page created
    bufMgr->unPinPage(file, newPageId, false);
    // value to push up
    // right biased
    // always going to be the middle value which is the 3rd array value of left node
    int pushedKey = false;
    // value to push up
    // right biased
    if (addedNewNode) {
        pushedKey = curNode->keyArray[INTARRAYLEAFSIZE / 2];
    } else {
        pushedKey = newNode->keyArray[0];
    }

    // check if parentId is null, if so call create new root
    if (curNode->parentId == NULL) {
        bool aboveLeaf = false;
        if (curNode->level == 1) {
            aboveLeaf = true;
        }
        // current node pid
        PageId leftChild = pid;
        // new node created pageID
        PageId rightChild = newPageId;
        // call create new root node
        createNewRoot((void *)pushedKey, leftChild, rightChild, false);
        // update the parentId of the two nodes created
    }
    // parent already exisits

    else {
        // update parent node to have a new child
        // declare parent page
        Page *parentPage;
        // read parent
        bufMgr->readPage(this->file, curNode->parentId, parentPage);
        // create parent node
        NonLeafNodeInt *parentNode = (NonLeafNodeInt *)parentPage;
        // unpin parent
        bufMgr->unPinPage(this->file, curNode->parentId, parentPage);
        // check if parent has space available
        if (parentNode->spaceAvail > 0) {
            // unpin page
            bufMgr->unPinPage(this->file, curNode->parentId, parentPage);
            // insert value into parent
            insertIntoNonLeafNode(curNode->parentId, (void *)pushedKey);
        }
        // if no spaceAvail, call splitNonLeafNode
        else {
            splitNonLeafNode(curNode->parentId, (void *)pushedKey);
        }
    }
}

/**
 * This method is called when a node is at max capacity already so it needs to be split.
 * This method also called insert, splitNonLeafNode based on which case it is
 *
 * @param pid   Page ID of the result
 * @param rid   RecordId of the result
 * @param key   the void pointer of the key to be searched
 */
void BTreeIndex::splitLeafNode(const void *key, const RecordId rid, const PageId pid) {
    // right biased
    // creates page of leaf node
    Page *leafPage;
    // reads the page to split
    bufMgr->readPage(this->file, pid, leafPage);
    // create node
    LeafNodeInt *curNode = (LeafNodeInt *)leafPage;
    // create new node to split into
    Page *newLeafPage;
    PageId newLeafPageId;
    // creates new page for split
    bufMgr->allocPage(this->file, newLeafPageId, newLeafPage);
    // create a new node
    LeafNodeInt *splitNode = (LeafNodeInt *)newLeafPage;
    // split leafNode into size of node / 2, (size of node / 2)+1
    for (int i = 0; i < INTARRAYLEAFSIZE / 2; i++) {
        // sets the split node to the second half values of the current node
        splitNode->keyArray[i] = curNode->keyArray[INTARRAYLEAFSIZE / 2 + i];
        splitNode->ridArray[i] = curNode->ridArray[INTARRAYLEAFSIZE / 2 + i];

        // delete the values from the current Node after adding to split node
        // ! We Actually don't have to delete them i don't think, since we keep track of the size, we will never use these values
        // curNode->keyArray[INTARRAYLEAFSIZE / 2 + i] = NULL;
        // curNode->ridArray[INTARRAYLEAFSIZE / 2 + i] = NULL;
    }
    // update split node attributes
    splitNode->spaceAvail = INTARRAYLEAFSIZE - INTARRAYLEAFSIZE / 2;
    splitNode->rightSibPageNo = curNode->rightSibPageNo;
    splitNode->parentId = curNode->parentId;
    // update all the attributes
    curNode->rightSibPageNo = newLeafPageId;
    curNode->spaceAvail = INTARRAYLEAFSIZE - INTARRAYLEAFSIZE / 2;
    bool addedNewNode = false;
    if (curNode->keyArray[INTARRAYLEAFSIZE / 2 - 1] >= *((int *)key)) {
        // call insert for the curNode
        insertIntoLeafNode(pid, rid, key);
    }
    // else call insert for the splitNode
    else {
        insertIntoLeafNode(newLeafPageId, rid, key);
        addedNewNode = true;
    }
    int pushedKey;
    // value to push up
    // right biased
    if (addedNewNode) {
        pushedKey = curNode->keyArray[INTARRAYLEAFSIZE / 2];
    } else {
        pushedKey = splitNode->keyArray[0];
    }

    // check if parentId is null, if so call create new root
    if (curNode->parentId == NULL) {
        // current node pid
        PageId leftChild = pid;
        // new node created pageID
        PageId rightChild = newLeafPageId;
        // call create new root node
        createNewRoot((void *)pushedKey, leftChild, rightChild, true);
        // update the parentId of the two nodes created
    }
    // parent already exists
    else {
        // update parent node to have a new child
        // declare parent page
        Page *parentPage;
        // read parent
        bufMgr->readPage(this->file, curNode->parentId, parentPage);
        // create parent node
        NonLeafNodeInt *parentNode = (NonLeafNodeInt *)parentPage;
        // unpin parent
        bufMgr->unPinPage(this->file, curNode->parentId, parentPage);
        // check if parent has space available
        if (parentNode->spaceAvail > 0) {
            // add newNode as child of parent node
            insertIntoNonLeafNode(curNode->parentId, (void *)pushedKey);
        }
        // if no spaceAvail, call splitNonLeafNode
        else {
            splitNonLeafNode(curNode->parentId, (void *)pushedKey);
        }
    }
}

/**
 * Begin a filtered scan of the index.  For instance, if the method is called
 * using ("a",GT,"d",LTE) then we should seek all entries with a value
 * greater than "a" and less than or equal to "d".
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
 *
 * @param lowVal	Low value of range, pointer to integer / double / char string
 * @param lowOp		Low operator (GT/GTE)
 * @param highVal	High value of range, pointer to integer / double / char string
 * @param highOp	High operator (LT/LTE)
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
 * @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
 **/
void BTreeIndex::startScan(const void *lowValParm, const Operator lowOpParm,
                           const void *highValParm, const Operator highOpParm) {
    if ((lowOpParm != GT && lowOpParm != GTE) ||
        (highOpParm != LT && highOpParm != LTE)) {
        throw BadOpcodesException();
    }

    lowOp = lowOpParm;
    highOp = highOpParm;

    // Have to cast to int pointer first and then reference to get int value
    lowValInt = *(int *)lowValParm;
    highValInt = *(int *)highOpParm;

    // Make sure the parameter makes sense
    if (lowValInt > highValInt) throw BadScanrangeException();

    // only one scan at a time
    if (scanExecuting) endScan();

    /** ########### begin scan ########### */
    scanExecuting = true;
}

/**
 * Fetch the record id of the next index entry that matches the scan.
 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
 * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
 **/
void BTreeIndex::scanNext(RecordId &outRid) {
    // Can't scan next if there's no scan going on
    if (!scanExecuting) throw ScanNotInitializedException();
    // Finishing scan
    if (nextEntry == -1) throw IndexScanCompletedException();

    LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;
    // If the current page is fully read, then move on to the right riblings page if possible.
    if (currentNode->ridArray[nextEntry].page_number == 0 || nextEntry == leafOccupancy) {
        if (currentNode->rightSibPageNo == 0) {
            throw IndexScanCompletedException();
        } else {
            bufMgr->unPinPage(file, currentPageNum, false);  // move on to right sibling's page.
            currentPageNum = currentNode->rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            nextEntry = 0;  // reset the entry to start at the first entry for the sibling's page.
            currentNode = (LeafNodeInt *)currentPageData;
        }
        // else, if the page is not fully read then you check if the retrieved key meets the conditions.
    } else {
        if (currentNode->ridArray[nextEntry].page_number != 0 && nextEntry != leafOccupancy) {
            int currentKey = currentNode->keyArray[nextEntry];
            int key = currentNode->keyArray[nextEntry];
            if (keyCorrect(lowOp, highOp, lowValInt, highValInt, key)) {
                outRid = currentNode->ridArray[nextEntry];
                nextEntry = nextEntry + 1;
            }
        } else {
            // the rid is not satisfied.
        }
    }
}

/**
 * @brief Checks if the key satisfies the conditions based on the operators and values.
 *
 * @param lowOp Low operator (GT/GTE)
 * @param highOp High operator (LT/LTE)
 * @param lowVal Low value of range, pointer to integer / double / char string
 * @param highVal High value of range, pointer to integer / double / char string
 * @param key The key to check.
 * @return true If the key meets the conditions
 * @return false if the key doesn't meet the conditions.
 */
bool BTreeIndex::keyCorrect(Operator lowOp, Operator highOp, int lowVal, int highVal, int key) {
    if (lowOp == GTE) {
        if (highOp == LTE) {
            return key <= highVal && key >= lowVal;
        } else if (highOp == LT) {
            return key < highVal && key >= lowVal;
        }
    } else {
        if (lowOp == GT) {
            if (highOp == LTE) {
                return key <= highVal && key > lowVal;
            } else {
                if (highOp == LT) {
                    return key < highVal && key > lowVal;
                }
            }
        }
    }
}
/**
 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
 * @throws ScanNotInitializedException If no scan has been initialized.
 **/
void BTreeIndex::endScan() {
    if (!scanExecuting) {  // Scan is not initialized.
        throw ScanNotInitializedException();
    }
    // reset scan specific variables.
    scanExecuting = false;
    nextEntry = -1;
    scanExecuting = false;
    bufMgr->unPinPage(file, currentPageNum, false);
    currentPageNum = (PageId)-1;
    currentPageData = NULL;
    lowOp = (Operator)NULL;
    highOp = (Operator)NULL;
    lowValInt = -1;
    highValInt = -1;
}

}  // namespace badgerdb
