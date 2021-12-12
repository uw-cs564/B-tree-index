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

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

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

        // create a meta page for the new index.
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
            } catch (EndOfFileException EOFE) {
                break;
            }
        }
        // finished with the meta page and root page and we did modify it.
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
    }
    // bufMgrIn->readPage()
    // if (relationName != meta)
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
    // The destructor should end any scan, clear state variables, unpin pinned pages, and flush index file,
    // and deletes the file object.
    if (scanExecuting) {
        try {
            endScan();
        } catch (ScanNotInitializedException) {
            std::cout << "No scan has been initialized.";
        }
    }
    scanExecuting = false;
    bufMgr->flushFile(file);
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    if (insertInRoot) {
        // case 1 - there hasn't been a split yet, ie, all nodes inserted into root node (leaf node)
        // call insert directly
        insertIntoLeafNode(rootPageNum, rid, key);
    } else {
        // case 2 - traverse tree till you find a leaf node to insert into
        PageId resultPage;
        searchNode(resultPage, key, rootPageNum);

        // then call insert on the result
        insertIntoLeafNode(resultPage, rid, key);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertIntoNode
// this method is for when where is space within a node
// It finds the correct spot to insert and then inserts a node
// -----------------------------------------------------------------------------
void BTreeIndex::insertIntoNode(const int PageId, NonLeafNodeInt currNode, const void *key, const RecordId) {
    // insert end of the key array
    int size = currNode->keyArray.size;
    currNode->keyArray.add(*key);
    // for loop to traverse through the keyArray
    // so it is in sorted order
    for (int i = 0; i < size; i++) {
        if (currNode->keyArray[size - i] < currNode->keyArray[size - i - 1])
            // swap key values
            int temp = currNode->keyArray[size - i - 1];
        currNode->keyArray[size - i - 1] = currNode->keyArray[size - i];
        currNode->keyArray[size - i] = temp;
        // swap rid values if it is a leaf node
        if (currNode->isNonLeaf) {
            int tempRid = currNode->ridArray[size - i - 1];
            currNode->ridArray[size - i - 1] = currNode->keyArray[size - i];
            currNode->ridArray[size - i] = temp;
        }
        // decrement the availSpace
        currNode->availSpace--;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertIntoLeafNode
// this method is for when inserting into a leaf - checks where it fits and insert
// calls split if nodes is full
// calls the insertintoNode method to sort it
// -----------------------------------------------------------------------------
void BTreeIndex::insertIntoLeafNode(const PageId pid, const RecordId rid, const void *key) {
    // Read the current page
    Page *curPage;
    bufMgr->readPage(file, pid, curPage);

    // the leaf node what we're insert into
    LeafNodeInt *curNode = (LeafNodeInt *)pid;

    if (curNode->spaceAvail > 0) {
        // If there's room in this node
        int numPage = INTARRAYLEAFSIZE - curNode->spaceAvail;  // How many pages are in this node

        // First add the new entry to the end of the array
        curNode->ridArray[numPage] = rid;
        curNode->keyArray[numPage] = *((int *)key);

        // Then bubble sort the entries into the right place
        for (int i = numPage; i > 0; i++) {
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
            // use up one aviliable space
            curNode->spaceAvail--;
            bufMgr->unPinPage(file, pid, true);
        }
    } else {
        // No room now, need to split and push up
        bufMgr->unPinPage(file, pid, true);
        splitLeafNode(key, rid, pid);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::create new Root
// this method is for when to create a new Root node while propograting
// -----------------------------------------------------------------------------
void BTreeIndex::createNewRoot(const int PageId Page, const void *key, const RecordId rid, const PageId leftChild, const PageId rightChild, bool aboveLeaf) {
    // root page should always be 2
    PageId rootId = Page;
    Page *rootPage;
    // create non lead node for root
    NonLeafNodeInt rootNode;
    bufMgr->readPage(this->file, rootId, rootPage);
    // itialize new root node
    rootNode = (NonLeafNodeInt *)rootPage;
    // update new non leaf node --> what to check?
    if (rootNode->isNonLeaf) {
        rootNode->level = 0;
    } else {
        rootNode->level = 1;
    }
    rootNode->keyArray.add(*key);
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
    // do we need to update the meta?
}
// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNodes -  None Leaf nodes
// called if spaceAvail = 0 when inserting
// -----------------------------------------------------------------------------
void BTreeIndex::createNewRoot(const void *key, const RecordId rid, const PageId leftChild, const PageId rightChild, bool aboveLeaf, int level) {
    Page *currPage;
    // reads the page to split
    bufMgr->readPage(this->file, Page, currPage);
    // itilaize the non leaf node to split
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)currPage;
    // Create the new page(sibling)
    Page *newPage;
    PageId newPageId;
    // allocate page
    this->bufMgr->allocPage(this->file, newPageId, newPage);
    // create node
    // assign variables to sibling
    sibling->level = currNode->level;
    sibling->parentId = currNode->parentId;
    // insert the first two node values
    sibling->keyArray[0] = currNode->keyArray[0];
    sibling->keyArray[1] = currNode->keyArray[1];
    // assign the child page numbers to siblings
    sibling->pageNoArray[0] = currNode->pageNoArray[0];
    sibling->pageNoArray[1] = currNode->pageNoArray[1];
    // call insert into node
    insertIntoLeaf(currNode->parentPage, parent);
    // remove node?

    // assign children

    // reassign variables of the nodes

    // if parent page Id = 0 then call the newRootMethod
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNode -  Leaf nodes
// -----------------------------------------------------------------------------

void BTreeIndex::splitLeafNode(const void *key, const RecordId rid, PageId pid) {
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
        curNode->keyArray[INTARRAYLEAFSIZE / 2 + i] = 0;
        curNode->ridArray[INTARRAYLEAFSIZE / 2 + i] = 0;
    }
    // update split node attributes
    splitNode->spaceAvail = INTARRAYLEAFSIZE - INTARRAYLEAFSIZE / 2;
    splitNode->rightSibPageNo = curNode->rightSibPageNo;
    splitNode->parentPage = curNode->parentPage
                                // update all the attributes
                                curNode->rightSibPageNo = newLeafPageId;
    curNode->spaceAvail = INTARRAYLEAFSIZE - INTARRAYLEAFSIZE / 2;
    if (curNode->keyArray[INTARRAYLEAFSIZE / 2 - 1] = > *key) {
        // call insert for the curNode
        insertIntoLeafNode(RecordId, *key, NonLeafNodeInt curNode);
    }
    // else call insert for the splitNode
    else {
        insertIntoLeafNode(RecordId, *key, NonLeafNodeInt splitNode);
    }
    // update parent node children array to contain splitNode pageID
    Page *parentPage;
    bufMgr->readPage(this->file, curNode->parentId, parentPage);
    // initialize parent node
    NonLeafNodeInt *parNode = (NonLeafNodeInt *)parentPage;
    // update parNode - children array to include split node
    parNode->pageNoArray.add(newLeafPageId);
    // check if there is room in parent to insert
    if (parNode->spaceAvail > 0) {
        // call insert on parent node
        insertIntoNode(curNode->parentId, RecordId, *key, parNode);
    }
    // if there is no room in parent node, call splitNonLeafNode
    else {
        // call split non leaf node
        splitNonLeafNode(curNode->parentId, parNode, *key, rid, const PageId leftChild, const PageId rightChild)
    }

    // unpin parent page
    this->bufMgr->unPinPage(this->file, newLeafPageId, true);
    // if the key to insert is less than the final value of curNode

    // read page of parent id
    // create parent node

    // check if there is space avail
    // call insert
    // else call split node

    // push up the first index of the splitNode after insertion
    // tree is right biased

    // call insert into NonLeafNode

    // check if the above Node has space or call split Node
}

// -----------------------------------------------------------------------------
// BTreeIndex::searchNode
// -----------------------------------------------------------------------------
void BTreeIndex::searchNode(PageId &pid, const void *key, PageId currentId) {
    // Reads the content of currentId into curPage
    Page *curPage;
    bufMgr->readPage(file, currentId, curPage);

    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void *lowValParm,
                           const Operator lowOpParm,
                           const void *highValParm,
                           const Operator highOpParm) {
    if ((lowOpParm != 'GT' && lowOpParm != 'GTE') ||
        (highOpParm != 'LT' && highOpParm != 'LTE')) {
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

void BTreeIndex::scanNext(RecordId &outRid) {
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }
    LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;
}

void BTreeIndex::scanNext(RecordId &outRid) {
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }
    LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;

    if (nextEntry == -1) throw IndexScanCompletedException();
    LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;
    if (currentNode->ridArray[nextEntry].page_number == 0 || nextEntry == leafOccupancy) {
        if (currentNode->rightsibPageNo == 0) {
            throw IndexScanCompletedExceptin()
        } else {
            bufMgr->unPinPage(file, currentPageNum, false);
            currentPageNum = currentNode->rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            nextEntry = 0;
            currentNode = (LeafNodeInt *)currentPageData;
        }
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

nextEntry = false;
scanExecuting = false;
bufMgr->unPinPage(file, currentPageNum, false);
currentPageNum = (PageId)-1;
currentPageData = nullptr;
}

}  // namespace badgerdb
